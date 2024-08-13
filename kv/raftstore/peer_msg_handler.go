package raftstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// read the pseudocode in the document
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// 1. 是否当前 peer 发生了选举有关的状态变更
	//   - Leader 变了或者 当前 peer 的 state变了
	//   - term 变了或者进行了投票
	// 2. 有需要应用的快照？快照怎么应用，它的哪部分数据的快照
	// 3. 有需要发送的 mesg
	// 4. 有需要应用的快照，raftlog 里面，applyIndex 到 commitIndex 这部分日志
	// 5. 有需要持久化的日志，
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		applySnapRes, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}

		if applySnapRes != nil {
			if !reflect.DeepEqual(applySnapRes.Region, applySnapRes.PrevRegion) {
				d.peerStorage.SetRegion(applySnapRes.Region)
				// TODO: ctx?
				d.ctx.storeMeta.Lock()
				d.ctx.storeMeta.regions[applySnapRes.Region.Id] = applySnapRes.Region
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{region: applySnapRes.PrevRegion})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapRes.Region})
				d.ctx.storeMeta.Unlock()
			}
		}

		d.Send(d.ctx.trans, ready.Messages)

		d.ApplyCommitedEntries(ready.CommittedEntries)
		d.RaftGroup.Advance(ready)
	}

}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	// 外部接收
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	// client 或者自身
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	// 驱动 RawNode 的 tick
	case message.MsgTypeTick:
		d.onTick()
	// 出发 region split
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	// 清理 snapshot
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	// 启动 peer，新建 peer 需要进行启动
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if msg.AdminRequest == nil {
		d.proposeClientCommand(msg, cb)
	} else {
		d.proposeAdminCommand(msg, cb)
	}
}

// 处理来自 client 的请求
func (d *peerMsgHandler) proposeClientCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	for _, req := range msg.Requests {
		var key []byte
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		}
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
			cb.Done(ErrResp(err))
			continue
		}
		rq := &raft_cmdpb.RaftCmdRequest{
			Header: msg.GetHeader(),
			Requests: []*raft_cmdpb.Request{
				req,
			},
		}

		data, err1 := rq.Marshal()

		if err1 != nil {
			log.Panic(err)
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.Propose(data)
	}
}

func (d *peerMsgHandler) proposeAdminCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {

	// propose a raft comm
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
			cb.Done(ErrResp(err))
			return
		}

		err = d.RaftGroup.Propose(data)
		if err != nil {
			log.Panic(err)
			cb.Done(ErrResp(err))
			return
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)

	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.GetPeer().GetId())

		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		}

		cb.Done(resp)

	case raft_cmdpb.AdminCmdType_ChangePeer:
		data, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
			cb.Done(ErrResp(err))
			return
		}

		// ignore the duplicate confchange command
		if msg.Header != nil {
			startEpoch := msg.GetHeader().GetRegionEpoch()
			if startEpoch != nil && util.IsEpochStale(startEpoch, d.Region().RegionEpoch) {
				resp := ErrResp(&util.ErrEpochNotMatch{})
				cb.Done(resp)
				return
			}
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		fmt.Println(p.index, p.term)
		confChange := eraftpb.ConfChange{
			ChangeType: req.GetChangePeer().GetChangeType(),
			NodeId:     req.GetChangePeer().GetPeer().GetId(),
			Context:    data,
		}

		err = d.RaftGroup.ProposeConfChange(confChange)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}

	case raft_cmdpb.AdminCmdType_Split:
		err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
		}

		data, err := msg.Marshal()
		if err != nil {
			cb.Done(ErrResp(err))
			log.Panic(err)
			return
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.Propose(data)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func (d *peerMsgHandler) ApplyCommitedEntries(commitedEntries []pb.Entry) {
	kvWB := &engine_util.WriteBatch{}
	for _, entry := range commitedEntries {
		d.peerStorage.applyState.AppliedIndex = entry.Index

		if entry.EntryType == eraftpb.EntryType_EntryConfChange {
			d.ApplyConfChangeEntry(&entry, kvWB)
		} else {
			d.ApplyCommitedEntry(&entry, kvWB)
		}

		if d.stopped {
			return
		}

		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
		}

	}
	kvWB.WriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) ApplyConfChangeEntry(commitedEntry *pb.Entry, kvWB *engine_util.WriteBatch) {
	confchange := &eraftpb.ConfChange{}
	err := confchange.Unmarshal(commitedEntry.Data)
	if err != nil {
		log.Panic(err)
	}

	req := &raft_cmdpb.RaftCmdRequest{}
	err = req.Unmarshal(confchange.Context)
	if err != nil {
		log.Panic(err)
	}

	// dumplicate confchange entry or out of date confchange entry
	if req.Header != nil {
		lastEpoch := req.Header.RegionEpoch
		if lastEpoch != nil && util.IsEpochStale(lastEpoch, d.Region().RegionEpoch) {
			resp := ErrResp(&util.ErrEpochNotMatch{})
			d.processProposals(resp, commitedEntry, false)
			return
		}
	}

	d.RaftGroup.ApplyConfChange(*confchange)

	switch confchange.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if !d.peerExists(confchange.NodeId) {
			d.Region().RegionEpoch.ConfVer++
			d.Region().Peers = append(d.Region().Peers, req.AdminRequest.ChangePeer.Peer)
			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

			d.ctx.storeMeta.Lock()
			// no need
			// d.ctx.storeMeta.regions[confchange.NodeId] = d.Region()
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			d.ctx.storeMeta.Unlock()

			// peercache
			d.insertPeerCache(req.AdminRequest.ChangePeer.Peer)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		// special case
		// when left two node, remove one,
		// in this case we should transfer leader
		if len(d.Region().Peers) == 2 && confchange.NodeId == req.AdminRequest.ChangePeer.Peer.Id {
			for _, p := range d.Region().Peers {
				if p.Id != req.AdminRequest.ChangePeer.Peer.Id {
					d.RaftGroup.TransferLeader(p.Id)
					break
				}
			}
		}

		if confchange.NodeId == d.PeerId() {
			if d.MaybeDestroy() {
				kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
				// this func will clear meta, and set peer to tombstone
				d.destroyPeer()
			}
		} else if d.peerExists(confchange.NodeId) {
			d.ctx.storeMeta.Lock()
			d.Region().RegionEpoch.ConfVer++
			newPeers := make([]*metapb.Peer, 0)
			for _, peer := range d.Region().Peers {
				if peer.Id != confchange.NodeId {
					newPeers = append(newPeers, peer)
				}
			}
			d.Region().Peers = newPeers

			meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)

			// d.ctx.storeMeta.regions[d.regionId] = d.Region()
			// peercache
			d.removePeerCache(confchange.NodeId)
			d.ctx.storeMeta.Unlock()

		}
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		},
	}

	d.processProposals(resp, commitedEntry, false)

	// d.notifyHeartbeatScheduler(d.Region(),d.peer)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

// one entry is a raft_cmdpb.Request, as we handle in proposeRaftCommand
func (d *peerMsgHandler) ApplyCommitedEntry(commitedEntry *pb.Entry, kvWB *engine_util.WriteBatch) {
	req := &raft_cmdpb.RaftCmdRequest{}
	err := req.Unmarshal(commitedEntry.GetData())
	if err != nil {
		panic(err)
	}
	if len(req.Requests) > 0 {
		d.handleNormalCommand(req, commitedEntry, kvWB)
	}

	// handle admin command
	if req.AdminRequest != nil {
		rq := req.AdminRequest
		switch rq.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.handleCompact(rq, commitedEntry, kvWB)
		case raft_cmdpb.AdminCmdType_Split:
			d.handleSplit(req, rq, commitedEntry, kvWB)
		}

	}
}

func (d *peerMsgHandler) handleSplit(req *raft_cmdpb.RaftCmdRequest, rq *raft_cmdpb.AdminRequest, entry *pb.Entry, kvWB *engine_util.WriteBatch) {
	fmt.Println(req.Header.RegionId, rq.Split.NewRegionId)

	// Header.RegionId 是 new region id?
	if req.Header.RegionId != d.regionId {
		resp := ErrResp(&util.ErrRegionNotFound{RegionId: req.Header.RegionId})
		d.processProposals(resp, entry, false)
		return
	}

	// 检测距离 client 发送命令时，这段时间内集群变更有没有配置改变
	// 过期
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if err != nil {
		resp := ErrResp(err)
		d.processProposals(resp, entry, false)
		return
	}

	// splitkey 是不是在当前 region 里面
	err = util.CheckKeyInRegion(rq.Split.SplitKey, d.Region())
	if err != nil {
		resp := ErrResp(err)
		d.processProposals(resp, entry, false)
		return
	}

	// 
	if len(rq.Split.NewPeerIds) != len(d.Region().Peers) {
		resp := ErrRespStaleCommand(req.Header.Term)
		d.processProposals(resp, entry, false)
		return
	}

	log.Infof("Region %d peer %d begin to split", d.regionId, d.PeerId())

	newPeers := make([]*metapb.Peer, 0)
	for i, pr := range d.Region().Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      rq.Split.NewPeerIds[i],
			StoreId: pr.StoreId,
		})
	}

	newRegion := &metapb.Region{
		Id:       rq.Split.NewRegionId,
		StartKey: rq.Split.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 0,
			Version: 0,
		},
		Peers: newPeers,
	}

	d.ctx.storeMeta.Lock()
	d.Region().RegionEpoch.Version++
	newRegion.RegionEpoch.Version++
	d.Region().EndKey = rq.Split.SplitKey
	d.ctx.storeMeta.regions[rq.Split.NewRegionId] = newRegion
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
	d.ctx.storeMeta.Unlock()

	// newPeers 只包含 peerid 和 peer 属于哪个 store
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panic(err)
	}

	newPeer.peerStorage.SetRegion(newRegion)
	d.ctx.router.register(newPeer)
	startMsg := message.Msg{
		RegionID: rq.Split.NewRegionId,
		Type:     message.MsgTypeStart,
	}
	err = d.ctx.router.send(rq.Split.NewRegionId, startMsg)
	if err != nil {
		log.Panic(err)
	}

	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{newRegion, d.Region()},
			},
		},
	}

	d.processProposals(resp, entry, false)

	d.notifyHeartbeatScheduler(d.Region(), d.peer)
	d.notifyHeartbeatScheduler(newRegion, newPeer)
}

func (d *peerMsgHandler) handleCompact(rq *raft_cmdpb.AdminRequest, commitedEntry *pb.Entry, kvWB *engine_util.WriteBatch) {
	compactLog := rq.GetCompactLog()
	compactIndex := compactLog.CompactIndex
	compactTerm := compactLog.CompactTerm
	if compactIndex > d.peerStorage.truncatedIndex() {
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactTerm
		err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
			return
		}
		d.ScheduleCompactLog(compactIndex)
	}

	adminResp := &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogResponse{},
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:        &raft_cmdpb.RaftResponseHeader{},
		AdminResponse: adminResp,
	}

	d.processProposals(cmdResp, commitedEntry, false)
}

func (d *peerMsgHandler) handleAdminCommand() {

}

func (d *peerMsgHandler) handleNormalCommand(req *raft_cmdpb.RaftCmdRequest, commitedEntry *pb.Entry, kvWB *engine_util.WriteBatch) {
	rq := req.Requests[0]
	resps := []*raft_cmdpb.Response{}

	switch rq.CmdType {
	case raft_cmdpb.CmdType_Get:
		value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, rq.Get.Cf, rq.Get.Key)

		resps = append(resps, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Get,
			Get:     &raft_cmdpb.GetResponse{Value: value},
		})

	case raft_cmdpb.CmdType_Put:
		kvWB.SetCF(rq.Put.Cf, rq.Put.Key, rq.Put.Value)

		resps = append(resps, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Put,
			Put:     &raft_cmdpb.PutResponse{},
		})

	case raft_cmdpb.CmdType_Delete:
		kvWB.DeleteCF(rq.Delete.Cf, rq.Delete.Key)

		resps = append(resps, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Delete,
			Delete:  &raft_cmdpb.DeleteResponse{},
		})

	case raft_cmdpb.CmdType_Snap:
		resps = append(resps, &raft_cmdpb.Response{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap: &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			},
		})
	}

	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: resps,
	}

	d.processProposals(cmdResp, commitedEntry, rq.CmdType == raft_cmdpb.CmdType_Snap)
}

func (d *peerMsgHandler) processProposals(resp *raft_cmdpb.RaftCmdResponse, entry *eraftpb.Entry, isExecSnap bool) {
	length := len(d.proposals)
	if length > 0 {
		d.dropStaleProposal(entry)
		if len(d.proposals) == 0 {
			return
		}
		p := d.proposals[0]
		if p.index > entry.Index {
			return
		}
		// term 不匹配
		if p.term != entry.Term {
			NotifyStaleReq(entry.Term, p.cb)
			d.proposals = d.proposals[1:]
			return
		}
		// p.index == entry.Index && p.term == entry.Term， 开始执行并回应
		if isExecSnap {
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false) // 注意时序，一定要在 Done 之前完成
		}
		p.cb.Done(resp)
		d.proposals = d.proposals[1:]
		return
	}
}

// 丢弃过时的 proposal
// 除了在 ApplyCommitedEntries 时，还会在其他地方 append proposals
func (d *peerMsgHandler) dropStaleProposal(entry *eraftpb.Entry) {
	length := len(d.proposals)
	if length > 0 {
		first := 0
		// 前面未回应的都说明过时了, 直接回应过时错误
		for first < length {
			p := d.proposals[first]
			if p.index < entry.Index {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				first++
			} else {
				break
			}
		}
		if first == length {
			d.proposals = make([]*proposal, 0)
			return
		}
		d.proposals = d.proposals[first:]
	}
}

func (d *peerMsgHandler) peerExists(id uint64) bool {
	for _, p := range d.Region().Peers {
		if p.Id == id {
			return true
		}
	}
	return false
}
