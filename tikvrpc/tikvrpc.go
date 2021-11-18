// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tikvrpc/tikvrpc.go
//

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvrpc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdCleanup
	CmdBatchGet
	CmdBatchRollback
	CmdScanLock
	CmdResolveLock
	CmdGC
	CmdDeleteRange
	CmdPessimisticLock
	CmdPessimisticRollback
	CmdTxnHeartBeat
	CmdCheckTxnStatus
	CmdCheckSecondaryLocks

	CmdRawGet CmdType = 256 + iota
	CmdRawBatchGet
	CmdRawPut
	CmdRawBatchPut
	CmdRawDelete
	CmdRawBatchDelete
	CmdRawDeleteRange
	CmdRawScan
	CmdGetKeyTTL
	CmdRawCompareAndSwap

	CmdUnsafeDestroyRange

	CmdRegisterLockObserver
	CmdCheckLockObserver
	CmdRemoveLockObserver
	CmdPhysicalScanLock

	CmdStoreSafeTS
	CmdLockWaitInfo

	CmdCop CmdType = 512 + iota
	CmdCopStream
	CmdBatchCop
	CmdMPPTask
	CmdMPPConn
	CmdMPPCancel
	CmdMPPAlive

	CmdMvccGetByKey CmdType = 1024 + iota
	CmdMvccGetByStartTs
	CmdSplitRegion

	CmdDebugGetRegionProperties CmdType = 2048 + iota

	CmdEmpty CmdType = 3072 + iota
)

func (t CmdType) String() string {
	switch t {
	case CmdGet:
		return "Get"
	case CmdScan:
		return "Scan"
	case CmdPrewrite:
		return "Prewrite"
	case CmdPessimisticLock:
		return "PessimisticLock"
	case CmdPessimisticRollback:
		return "PessimisticRollback"
	case CmdCommit:
		return "Commit"
	case CmdCleanup:
		return "Cleanup"
	case CmdBatchGet:
		return "BatchGet"
	case CmdBatchRollback:
		return "BatchRollback"
	case CmdScanLock:
		return "ScanLock"
	case CmdResolveLock:
		return "ResolveLock"
	case CmdGC:
		return "GC"
	case CmdDeleteRange:
		return "DeleteRange"
	case CmdRawGet:
		return "RawGet"
	case CmdRawBatchGet:
		return "RawBatchGet"
	case CmdRawPut:
		return "RawPut"
	case CmdRawBatchPut:
		return "RawBatchPut"
	case CmdRawDelete:
		return "RawDelete"
	case CmdRawBatchDelete:
		return "RawBatchDelete"
	case CmdRawDeleteRange:
		return "RawDeleteRange"
	case CmdRawScan:
		return "RawScan"
	case CmdUnsafeDestroyRange:
		return "UnsafeDestroyRange"
	case CmdRegisterLockObserver:
		return "RegisterLockObserver"
	case CmdCheckLockObserver:
		return "CheckLockObserver"
	case CmdRemoveLockObserver:
		return "RemoveLockObserver"
	case CmdPhysicalScanLock:
		return "PhysicalScanLock"
	case CmdCop:
		return "Cop"
	case CmdCopStream:
		return "CopStream"
	case CmdBatchCop:
		return "BatchCop"
	case CmdMPPTask:
		return "DispatchMPPTask"
	case CmdMPPConn:
		return "EstablishMPPConnection"
	case CmdMPPCancel:
		return "CancelMPPTask"
	case CmdMPPAlive:
		return "MPPAlive"
	case CmdMvccGetByKey:
		return "MvccGetByKey"
	case CmdMvccGetByStartTs:
		return "MvccGetByStartTS"
	case CmdSplitRegion:
		return "SplitRegion"
	case CmdCheckTxnStatus:
		return "CheckTxnStatus"
	case CmdCheckSecondaryLocks:
		return "CheckSecondaryLocks"
	case CmdDebugGetRegionProperties:
		return "DebugGetRegionProperties"
	case CmdTxnHeartBeat:
		return "TxnHeartBeat"
	case CmdStoreSafeTS:
		return "StoreSafeTS"
	case CmdLockWaitInfo:
		return "LockWaitInfo"
	}
	return "Unknown"
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	Type CmdType
	Req  interface{}
	kvrpcpb.Context
	ReadReplicaScope string
	// remove txnScope after tidb removed txnScope
	TxnScope        string
	ReplicaReadType kv.ReplicaReadType // different from `kvrpcpb.Context.ReplicaRead`
	ReplicaReadSeed *uint32            // pointer to follower read seed in snapshot/coprocessor
	StoreTp         EndpointType
	// ForwardedHost is the address of a store which will handle the request. It's different from
	// the address the request sent to.
	// If it's not empty, the store which receive the request will forward it to
	// the forwarded host. It's useful when network partition occurs.
	ForwardedHost string
}

// NewRequest returns new kv rpc request.
func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request {
	if len(ctxs) > 0 {
		return &Request{
			Type:    typ,
			Req:     pointer,
			Context: ctxs[0],
		}
	}
	return &Request{
		Type: typ,
		Req:  pointer,
	}
}

// NewReplicaReadRequest returns new kv rpc request with replica read.
func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType kv.ReplicaReadType, replicaReadSeed *uint32, ctxs ...kvrpcpb.Context) *Request {
	req := NewRequest(typ, pointer, ctxs...)
	req.ReplicaRead = replicaReadType.IsFollowerRead()
	req.ReplicaReadType = replicaReadType
	req.ReplicaReadSeed = replicaReadSeed
	return req
}

// GetReplicaReadSeed returns ReplicaReadSeed pointer.
func (req *Request) GetReplicaReadSeed() *uint32 {
	if req != nil {
		return req.ReplicaReadSeed
	}
	return nil
}

// EnableStaleRead enables stale read
func (req *Request) EnableStaleRead() {
	req.StaleRead = true
	req.ReplicaReadType = kv.ReplicaReadMixed
	req.ReplicaRead = false
}

// IsGlobalStaleRead checks if the request is a global stale read request.
func (req *Request) IsGlobalStaleRead() bool {
	return req.ReadReplicaScope == oracle.GlobalTxnScope &&
		// remove txnScope after tidb remove it
		req.TxnScope == oracle.GlobalTxnScope &&
		req.GetStaleRead()
}

// IsDebugReq check whether the req is debug req.
func (req *Request) IsDebugReq() bool {
	switch req.Type {
	case CmdDebugGetRegionProperties:
		return true
	}
	return false
}

// Get returns GetRequest in request.
func (req *Request) Get() *kvrpcpb.GetRequest {
	return req.Req.(*kvrpcpb.GetRequest)
}

// Scan returns ScanRequest in request.
func (req *Request) Scan() *kvrpcpb.ScanRequest {
	return req.Req.(*kvrpcpb.ScanRequest)
}

// Prewrite returns PrewriteRequest in request.
func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest {
	return req.Req.(*kvrpcpb.PrewriteRequest)
}

// Commit returns CommitRequest in request.
func (req *Request) Commit() *kvrpcpb.CommitRequest {
	return req.Req.(*kvrpcpb.CommitRequest)
}

// Cleanup returns CleanupRequest in request.
func (req *Request) Cleanup() *kvrpcpb.CleanupRequest {
	return req.Req.(*kvrpcpb.CleanupRequest)
}

// BatchGet returns BatchGetRequest in request.
func (req *Request) BatchGet() *kvrpcpb.BatchGetRequest {
	return req.Req.(*kvrpcpb.BatchGetRequest)
}

// BatchRollback returns BatchRollbackRequest in request.
func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest {
	return req.Req.(*kvrpcpb.BatchRollbackRequest)
}

// ScanLock returns ScanLockRequest in request.
func (req *Request) ScanLock() *kvrpcpb.ScanLockRequest {
	return req.Req.(*kvrpcpb.ScanLockRequest)
}

// ResolveLock returns ResolveLockRequest in request.
func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest {
	return req.Req.(*kvrpcpb.ResolveLockRequest)
}

// GC returns GCRequest in request.
func (req *Request) GC() *kvrpcpb.GCRequest {
	return req.Req.(*kvrpcpb.GCRequest)
}

// DeleteRange returns DeleteRangeRequest in request.
func (req *Request) DeleteRange() *kvrpcpb.DeleteRangeRequest {
	return req.Req.(*kvrpcpb.DeleteRangeRequest)
}

// RawGet returns RawGetRequest in request.
func (req *Request) RawGet() *kvrpcpb.RawGetRequest {
	return req.Req.(*kvrpcpb.RawGetRequest)
}

// RawBatchGet returns RawBatchGetRequest in request.
func (req *Request) RawBatchGet() *kvrpcpb.RawBatchGetRequest {
	return req.Req.(*kvrpcpb.RawBatchGetRequest)
}

// RawPut returns RawPutRequest in request.
func (req *Request) RawPut() *kvrpcpb.RawPutRequest {
	return req.Req.(*kvrpcpb.RawPutRequest)
}

// RawBatchPut returns RawBatchPutRequest in request.
func (req *Request) RawBatchPut() *kvrpcpb.RawBatchPutRequest {
	return req.Req.(*kvrpcpb.RawBatchPutRequest)
}

// RawDelete returns PrewriteRequest in request.
func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest {
	return req.Req.(*kvrpcpb.RawDeleteRequest)
}

// RawBatchDelete returns RawBatchDeleteRequest in request.
func (req *Request) RawBatchDelete() *kvrpcpb.RawBatchDeleteRequest {
	return req.Req.(*kvrpcpb.RawBatchDeleteRequest)
}

// RawDeleteRange returns RawDeleteRangeRequest in request.
func (req *Request) RawDeleteRange() *kvrpcpb.RawDeleteRangeRequest {
	return req.Req.(*kvrpcpb.RawDeleteRangeRequest)
}

// RawScan returns RawScanRequest in request.
func (req *Request) RawScan() *kvrpcpb.RawScanRequest {
	return req.Req.(*kvrpcpb.RawScanRequest)
}

// UnsafeDestroyRange returns UnsafeDestroyRangeRequest in request.
func (req *Request) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeRequest {
	return req.Req.(*kvrpcpb.UnsafeDestroyRangeRequest)
}

// RawGetKeyTTL returns RawGetKeyTTLRequest in request.
func (req *Request) RawGetKeyTTL() *kvrpcpb.RawGetKeyTTLRequest {
	return req.Req.(*kvrpcpb.RawGetKeyTTLRequest)
}

// RawCompareAndSwap returns RawCASRequest in request.
func (req *Request) RawCompareAndSwap() *kvrpcpb.RawCASRequest {
	return req.Req.(*kvrpcpb.RawCASRequest)
}

// RegisterLockObserver returns RegisterLockObserverRequest in request.
func (req *Request) RegisterLockObserver() *kvrpcpb.RegisterLockObserverRequest {
	return req.Req.(*kvrpcpb.RegisterLockObserverRequest)
}

// CheckLockObserver returns CheckLockObserverRequest in request.
func (req *Request) CheckLockObserver() *kvrpcpb.CheckLockObserverRequest {
	return req.Req.(*kvrpcpb.CheckLockObserverRequest)
}

// RemoveLockObserver returns RemoveLockObserverRequest in request.
func (req *Request) RemoveLockObserver() *kvrpcpb.RemoveLockObserverRequest {
	return req.Req.(*kvrpcpb.RemoveLockObserverRequest)
}

// PhysicalScanLock returns PhysicalScanLockRequest in request.
func (req *Request) PhysicalScanLock() *kvrpcpb.PhysicalScanLockRequest {
	return req.Req.(*kvrpcpb.PhysicalScanLockRequest)
}

// Cop returns coprocessor request in request.
func (req *Request) Cop() *coprocessor.Request {
	return req.Req.(*coprocessor.Request)
}

// BatchCop returns BatchCop request in request.
func (req *Request) BatchCop() *coprocessor.BatchRequest {
	return req.Req.(*coprocessor.BatchRequest)
}

// DispatchMPPTask returns dispatch task request in request.
func (req *Request) DispatchMPPTask() *mpp.DispatchTaskRequest {
	return req.Req.(*mpp.DispatchTaskRequest)
}

// IsMPPAlive returns IsAlive request in request.
func (req *Request) IsMPPAlive() *mpp.IsAliveRequest {
	return req.Req.(*mpp.IsAliveRequest)
}

// EstablishMPPConn returns EstablishMPPConnectionRequest in request.
func (req *Request) EstablishMPPConn() *mpp.EstablishMPPConnectionRequest {
	return req.Req.(*mpp.EstablishMPPConnectionRequest)
}

// CancelMPPTask returns canceling task in request
func (req *Request) CancelMPPTask() *mpp.CancelTaskRequest {
	return req.Req.(*mpp.CancelTaskRequest)
}

// MvccGetByKey returns MvccGetByKeyRequest in request.
func (req *Request) MvccGetByKey() *kvrpcpb.MvccGetByKeyRequest {
	return req.Req.(*kvrpcpb.MvccGetByKeyRequest)
}

// MvccGetByStartTs returns MvccGetByStartTsRequest in request.
func (req *Request) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsRequest {
	return req.Req.(*kvrpcpb.MvccGetByStartTsRequest)
}

// SplitRegion returns SplitRegionRequest in request.
func (req *Request) SplitRegion() *kvrpcpb.SplitRegionRequest {
	return req.Req.(*kvrpcpb.SplitRegionRequest)
}

// PessimisticLock returns PessimisticLockRequest in request.
func (req *Request) PessimisticLock() *kvrpcpb.PessimisticLockRequest {
	return req.Req.(*kvrpcpb.PessimisticLockRequest)
}

// PessimisticRollback returns PessimisticRollbackRequest in request.
func (req *Request) PessimisticRollback() *kvrpcpb.PessimisticRollbackRequest {
	return req.Req.(*kvrpcpb.PessimisticRollbackRequest)
}

// DebugGetRegionProperties returns GetRegionPropertiesRequest in request.
func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest {
	return req.Req.(*debugpb.GetRegionPropertiesRequest)
}

// Empty returns BatchCommandsEmptyRequest in request.
func (req *Request) Empty() *tikvpb.BatchCommandsEmptyRequest {
	return req.Req.(*tikvpb.BatchCommandsEmptyRequest)
}

// CheckTxnStatus returns CheckTxnStatusRequest in request.
func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest {
	return req.Req.(*kvrpcpb.CheckTxnStatusRequest)
}

// CheckSecondaryLocks returns CheckSecondaryLocksRequest in request.
func (req *Request) CheckSecondaryLocks() *kvrpcpb.CheckSecondaryLocksRequest {
	return req.Req.(*kvrpcpb.CheckSecondaryLocksRequest)
}

// TxnHeartBeat returns TxnHeartBeatRequest in request.
func (req *Request) TxnHeartBeat() *kvrpcpb.TxnHeartBeatRequest {
	return req.Req.(*kvrpcpb.TxnHeartBeatRequest)
}

// StoreSafeTS returns StoreSafeTSRequest in request.
func (req *Request) StoreSafeTS() *kvrpcpb.StoreSafeTSRequest {
	return req.Req.(*kvrpcpb.StoreSafeTSRequest)
}

// LockWaitInfo returns GetLockWaitInfoRequest in request.
func (req *Request) LockWaitInfo() *kvrpcpb.GetLockWaitInfoRequest {
	return req.Req.(*kvrpcpb.GetLockWaitInfoRequest)
}

// ToBatchCommandsRequest converts the request to an entry in BatchCommands request.
func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request {
	switch req.Type {
	case CmdGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Get{Get: req.Get()}}
	case CmdScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Scan{Scan: req.Scan()}}
	case CmdPrewrite:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Prewrite{Prewrite: req.Prewrite()}}
	case CmdCommit:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Commit{Commit: req.Commit()}}
	case CmdCleanup:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Cleanup{Cleanup: req.Cleanup()}}
	case CmdBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchGet{BatchGet: req.BatchGet()}}
	case CmdBatchRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchRollback{BatchRollback: req.BatchRollback()}}
	case CmdScanLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ScanLock{ScanLock: req.ScanLock()}}
	case CmdResolveLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ResolveLock{ResolveLock: req.ResolveLock()}}
	case CmdGC:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_GC{GC: req.GC()}}
	case CmdDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_DeleteRange{DeleteRange: req.DeleteRange()}}
	case CmdRawGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawGet{RawGet: req.RawGet()}}
	case CmdRawBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchGet{RawBatchGet: req.RawBatchGet()}}
	case CmdRawPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawPut{RawPut: req.RawPut()}}
	case CmdRawBatchPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchPut{RawBatchPut: req.RawBatchPut()}}
	case CmdRawDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDelete{RawDelete: req.RawDelete()}}
	case CmdRawBatchDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchDelete{RawBatchDelete: req.RawBatchDelete()}}
	case CmdRawDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDeleteRange{RawDeleteRange: req.RawDeleteRange()}}
	case CmdRawScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawScan{RawScan: req.RawScan()}}
	case CmdCop:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: req.Cop()}}
	case CmdPessimisticLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticLock{PessimisticLock: req.PessimisticLock()}}
	case CmdPessimisticRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticRollback{PessimisticRollback: req.PessimisticRollback()}}
	case CmdEmpty:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{Empty: req.Empty()}}
	case CmdCheckTxnStatus:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_CheckTxnStatus{CheckTxnStatus: req.CheckTxnStatus()}}
	case CmdCheckSecondaryLocks:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_CheckSecondaryLocks{CheckSecondaryLocks: req.CheckSecondaryLocks()}}
	case CmdTxnHeartBeat:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_TxnHeartBeat{TxnHeartBeat: req.TxnHeartBeat()}}
	}
	return nil
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Resp               interface{}
	ServerSideDuration time.Duration
}

// FromBatchCommandsResponse converts a BatchCommands response to Response.
func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) (*Response, error) {
	if res.GetCmd() == nil {
		return nil, errors.New("Unknown command response")
	}
	switch res := res.GetCmd().(type) {
	case *tikvpb.BatchCommandsResponse_Response_Get:
		resp := &Response{Resp: res.Get}
		if res.Get != nil && res.Get.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.Get.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_Scan:
		resp := &Response{Resp: res.Scan}
		if res.Scan != nil && res.Scan.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.Scan.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_Prewrite:
		resp := &Response{Resp: res.Prewrite}
		if res.Prewrite != nil && res.Prewrite.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.Prewrite.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_Commit:
		resp := &Response{Resp: res.Commit}
		if res.Commit != nil && res.Commit.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.Commit.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_Cleanup:
		resp := &Response{Resp: res.Cleanup}
		if res.Cleanup != nil && res.Cleanup.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.Cleanup.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_BatchGet:
		resp := &Response{Resp: res.BatchGet}
		if res.BatchGet != nil && res.BatchGet.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.BatchGet.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_BatchRollback:
		resp := &Response{Resp: res.BatchRollback}
		if res.BatchRollback != nil && res.BatchRollback.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.BatchRollback.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_ScanLock:
		resp := &Response{Resp: res.ScanLock}
		if res.ScanLock != nil && res.ScanLock.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.ScanLock.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_ResolveLock:
		resp := &Response{Resp: res.ResolveLock}
		if res.ResolveLock != nil && res.ResolveLock.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.ResolveLock.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_GC:
		resp := &Response{Resp: res.GC}
		if res.GC != nil && res.GC.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.GC.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_DeleteRange:
		resp := &Response{Resp: res.DeleteRange}
		if res.DeleteRange != nil && res.DeleteRange.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.DeleteRange.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawGet:
		resp := &Response{Resp: res.RawGet}
		if res.RawGet != nil && res.RawGet.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawGet.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawBatchGet:
		resp := &Response{Resp: res.RawBatchGet}
		if res.RawBatchGet != nil && res.RawBatchGet.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawBatchGet.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawPut:
		resp := &Response{Resp: res.RawPut}
		if res.RawPut != nil && res.RawPut.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawPut.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawBatchPut:
		resp := &Response{Resp: res.RawBatchPut}
		if res.RawBatchPut != nil && res.RawBatchPut.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawBatchPut.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawDelete:
		resp := &Response{Resp: res.RawDelete}
		if res.RawDelete != nil && res.RawDelete.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawDelete.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawBatchDelete:
		resp := &Response{Resp: res.RawBatchDelete}
		if res.RawBatchDelete != nil && res.RawBatchDelete.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawBatchDelete.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawDeleteRange:
		resp := &Response{Resp: res.RawDeleteRange}
		if res.RawDeleteRange != nil && res.RawDeleteRange.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawDeleteRange.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_RawScan:
		resp := &Response{Resp: res.RawScan}
		if res.RawScan != nil && res.RawScan.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.RawScan.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_Coprocessor:
		resp := &Response{Resp: res.Coprocessor}
		if res.Coprocessor != nil && res.Coprocessor.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.Coprocessor.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_PessimisticLock:
		resp := &Response{Resp: res.PessimisticLock}
		if res.PessimisticLock != nil && res.PessimisticLock.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.PessimisticLock.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_PessimisticRollback:
		resp := &Response{Resp: res.PessimisticRollback}
		if res.PessimisticRollback != nil && res.PessimisticRollback.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.PessimisticRollback.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_Empty:
		return &Response{Resp: res.Empty}, nil

	case *tikvpb.BatchCommandsResponse_Response_TxnHeartBeat:
		resp := &Response{Resp: res.TxnHeartBeat}
		if res.TxnHeartBeat != nil && res.TxnHeartBeat.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.TxnHeartBeat.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_CheckTxnStatus:
		resp := &Response{Resp: res.CheckTxnStatus}
		if res.CheckTxnStatus != nil && res.CheckTxnStatus.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.CheckTxnStatus.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	case *tikvpb.BatchCommandsResponse_Response_CheckSecondaryLocks:
		resp := &Response{Resp: res.CheckSecondaryLocks}
		if res.CheckSecondaryLocks != nil && res.CheckSecondaryLocks.Meta != nil {
			resp.ServerSideDuration = time.Duration(res.CheckSecondaryLocks.Meta.ProcessDurationUs) * time.Microsecond
		}
		return resp, nil

	}
	panic("unreachable")
}

// CopStreamResponse combines tikvpb.Tikv_CoprocessorStreamClient and the first Recv() result together.
// In streaming API, get grpc stream client may not involve any network packet, then region error have
// to be handled in Recv() function. This struct facilitates the error handling.
type CopStreamResponse struct {
	tikvpb.Tikv_CoprocessorStreamClient
	*coprocessor.Response // The first result of Recv()
	Timeout               time.Duration
	Lease                 // Shared by this object and a background goroutine.
}

// BatchCopStreamResponse comprises the BatchCoprocessorClient , the first result and timeout detector.
type BatchCopStreamResponse struct {
	tikvpb.Tikv_BatchCoprocessorClient
	*coprocessor.BatchResponse
	Timeout time.Duration
	Lease   // Shared by this object and a background goroutine.
}

// MPPStreamResponse is indeed a wrapped client that can receive data packet from tiflash mpp server.
type MPPStreamResponse struct {
	tikvpb.Tikv_EstablishMPPConnectionClient
	*mpp.MPPDataPacket
	Timeout time.Duration
	Lease
}

// SetContext set the Context field for the given req to the specified ctx.
func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	ctx := &req.Context
	if region != nil {
		ctx.RegionId = region.Id
		ctx.RegionEpoch = region.RegionEpoch
	}
	ctx.Peer = peer

	switch req.Type {
	case CmdGet:
		req.Get().Context = ctx
	case CmdScan:
		req.Scan().Context = ctx
	case CmdPrewrite:
		req.Prewrite().Context = ctx
	case CmdPessimisticLock:
		req.PessimisticLock().Context = ctx
	case CmdPessimisticRollback:
		req.PessimisticRollback().Context = ctx
	case CmdCommit:
		req.Commit().Context = ctx
	case CmdCleanup:
		req.Cleanup().Context = ctx
	case CmdBatchGet:
		req.BatchGet().Context = ctx
	case CmdBatchRollback:
		req.BatchRollback().Context = ctx
	case CmdScanLock:
		req.ScanLock().Context = ctx
	case CmdResolveLock:
		req.ResolveLock().Context = ctx
	case CmdGC:
		req.GC().Context = ctx
	case CmdDeleteRange:
		req.DeleteRange().Context = ctx
	case CmdRawGet:
		req.RawGet().Context = ctx
	case CmdRawBatchGet:
		req.RawBatchGet().Context = ctx
	case CmdRawPut:
		req.RawPut().Context = ctx
	case CmdRawBatchPut:
		req.RawBatchPut().Context = ctx
	case CmdRawDelete:
		req.RawDelete().Context = ctx
	case CmdRawBatchDelete:
		req.RawBatchDelete().Context = ctx
	case CmdRawDeleteRange:
		req.RawDeleteRange().Context = ctx
	case CmdRawScan:
		req.RawScan().Context = ctx
	case CmdUnsafeDestroyRange:
		req.UnsafeDestroyRange().Context = ctx
	case CmdGetKeyTTL:
		req.RawGetKeyTTL().Context = ctx
	case CmdRawCompareAndSwap:
		req.RawCompareAndSwap().Context = ctx
	case CmdRegisterLockObserver:
		req.RegisterLockObserver().Context = ctx
	case CmdCheckLockObserver:
		req.CheckLockObserver().Context = ctx
	case CmdRemoveLockObserver:
		req.RemoveLockObserver().Context = ctx
	case CmdPhysicalScanLock:
		req.PhysicalScanLock().Context = ctx
	case CmdCop:
		req.Cop().Context = ctx
	case CmdCopStream:
		req.Cop().Context = ctx
	case CmdBatchCop:
		req.BatchCop().Context = ctx
	case CmdMPPTask:
		// Dispatching MPP tasks don't need a region context, because it's a request for store but not region.
	case CmdMvccGetByKey:
		req.MvccGetByKey().Context = ctx
	case CmdMvccGetByStartTs:
		req.MvccGetByStartTs().Context = ctx
	case CmdSplitRegion:
		req.SplitRegion().Context = ctx
	case CmdEmpty:
		req.SplitRegion().Context = ctx
	case CmdTxnHeartBeat:
		req.TxnHeartBeat().Context = ctx
	case CmdCheckTxnStatus:
		req.CheckTxnStatus().Context = ctx
	case CmdCheckSecondaryLocks:
		req.CheckSecondaryLocks().Context = ctx
	default:
		return errors.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	var p interface{}
	resp := &Response{}
	switch req.Type {
	case CmdGet:
		p = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		p = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		p = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdPessimisticLock:
		p = &kvrpcpb.PessimisticLockResponse{
			RegionError: e,
		}
	case CmdPessimisticRollback:
		p = &kvrpcpb.PessimisticRollbackResponse{
			RegionError: e,
		}
	case CmdCommit:
		p = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		p = &kvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		p = &kvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		p = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		p = &kvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		p = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		p = &kvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdDeleteRange:
		p = &kvrpcpb.DeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawGet:
		p = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawBatchGet:
		p = &kvrpcpb.RawBatchGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		p = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawBatchPut:
		p = &kvrpcpb.RawBatchPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		p = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawBatchDelete:
		p = &kvrpcpb.RawBatchDeleteResponse{
			RegionError: e,
		}
	case CmdRawDeleteRange:
		p = &kvrpcpb.RawDeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawScan:
		p = &kvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdUnsafeDestroyRange:
		p = &kvrpcpb.UnsafeDestroyRangeResponse{
			RegionError: e,
		}
	case CmdGetKeyTTL:
		p = &kvrpcpb.RawGetKeyTTLResponse{
			RegionError: e,
		}
	case CmdRawCompareAndSwap:
		p = &kvrpcpb.RawCASResponse{
			RegionError: e,
		}
	case CmdCop:
		p = &coprocessor.Response{
			RegionError: e,
		}
	case CmdCopStream:
		p = &CopStreamResponse{
			Response: &coprocessor.Response{
				RegionError: e,
			},
		}
	case CmdMvccGetByKey:
		p = &kvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		}
	case CmdMvccGetByStartTs:
		p = &kvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		}
	case CmdSplitRegion:
		p = &kvrpcpb.SplitRegionResponse{
			RegionError: e,
		}
	case CmdEmpty:
	case CmdTxnHeartBeat:
		p = &kvrpcpb.TxnHeartBeatResponse{
			RegionError: e,
		}
	case CmdCheckTxnStatus:
		p = &kvrpcpb.CheckTxnStatusResponse{
			RegionError: e,
		}
	case CmdCheckSecondaryLocks:
		p = &kvrpcpb.CheckSecondaryLocksResponse{
			RegionError: e,
		}
	default:
		return nil, errors.Errorf("invalid request type %v", req.Type)
	}
	resp.Resp = p
	return resp, nil
}

type getRegionError interface {
	GetRegionError() *errorpb.Error
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	if resp.Resp == nil {
		return nil, nil
	}
	err, ok := resp.Resp.(getRegionError)
	if !ok {
		if _, isEmpty := resp.Resp.(*tikvpb.BatchCommandsEmptyResponse); isEmpty {
			return nil, nil
		}
		return nil, errors.Errorf("invalid response type %v", resp)
	}
	return err.GetRegionError(), nil
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for coprocessor streaming, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdGet:
		kvResp, err1 := client.KvGet(ctx, req.Get())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdScan:
		kvResp, err1 := client.KvScan(ctx, req.Scan())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdPrewrite:
		kvResp, err1 := client.KvPrewrite(ctx, req.Prewrite())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdPessimisticLock:
		kvResp, err1 := client.KvPessimisticLock(ctx, req.PessimisticLock())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdPessimisticRollback:
		kvResp, err1 := client.KVPessimisticRollback(ctx, req.PessimisticRollback())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdCommit:
		kvResp, err1 := client.KvCommit(ctx, req.Commit())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdCleanup:
		kvResp, err1 := client.KvCleanup(ctx, req.Cleanup())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdBatchGet:
		kvResp, err1 := client.KvBatchGet(ctx, req.BatchGet())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdBatchRollback:
		kvResp, err1 := client.KvBatchRollback(ctx, req.BatchRollback())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdScanLock:
		kvResp, err1 := client.KvScanLock(ctx, req.ScanLock())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdResolveLock:
		kvResp, err1 := client.KvResolveLock(ctx, req.ResolveLock())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdGC:
		kvResp, err1 := client.KvGC(ctx, req.GC())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdDeleteRange:
		kvResp, err1 := client.KvDeleteRange(ctx, req.DeleteRange())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawGet:
		kvResp, err1 := client.RawGet(ctx, req.RawGet())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawBatchGet:
		kvResp, err1 := client.RawBatchGet(ctx, req.RawBatchGet())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawPut:
		kvResp, err1 := client.RawPut(ctx, req.RawPut())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawBatchPut:
		kvResp, err1 := client.RawBatchPut(ctx, req.RawBatchPut())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawDelete:
		kvResp, err1 := client.RawDelete(ctx, req.RawDelete())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawBatchDelete:
		kvResp, err1 := client.RawBatchDelete(ctx, req.RawBatchDelete())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawDeleteRange:
		kvResp, err1 := client.RawDeleteRange(ctx, req.RawDeleteRange())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawScan:
		kvResp, err1 := client.RawScan(ctx, req.RawScan())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdUnsafeDestroyRange:
		kvResp, err1 := client.UnsafeDestroyRange(ctx, req.UnsafeDestroyRange())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdGetKeyTTL:
		kvResp, err1 := client.RawGetKeyTTL(ctx, req.RawGetKeyTTL())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRawCompareAndSwap:
		kvResp, err1 := client.RawCompareAndSwap(ctx, req.RawCompareAndSwap())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRegisterLockObserver:
		kvResp, err1 := client.RegisterLockObserver(ctx, req.RegisterLockObserver())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdCheckLockObserver:
		kvResp, err1 := client.CheckLockObserver(ctx, req.CheckLockObserver())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdRemoveLockObserver:
		kvResp, err1 := client.RemoveLockObserver(ctx, req.RemoveLockObserver())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdPhysicalScanLock:
		kvResp, err1 := client.PhysicalScanLock(ctx, req.PhysicalScanLock())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdCop:
		kvResp, err1 := client.Coprocessor(ctx, req.Cop())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdMPPTask:
		resp.Resp, err = client.DispatchMPPTask(ctx, req.DispatchMPPTask())
	case CmdMPPAlive:
		resp.Resp, err = client.IsAlive(ctx, req.IsMPPAlive())
	case CmdMPPConn:
		var streamClient tikvpb.Tikv_EstablishMPPConnectionClient
		streamClient, err = client.EstablishMPPConnection(ctx, req.EstablishMPPConn())
		resp.Resp = &MPPStreamResponse{
			Tikv_EstablishMPPConnectionClient: streamClient,
		}
	case CmdMPPCancel:
		// it cannot use the ctx with cancel(), otherwise this cmd will fail.
		resp.Resp, err = client.CancelMPPTask(ctx, req.CancelMPPTask())
	case CmdCopStream:
		var streamClient tikvpb.Tikv_CoprocessorStreamClient
		streamClient, err = client.CoprocessorStream(ctx, req.Cop())
		resp.Resp = &CopStreamResponse{
			Tikv_CoprocessorStreamClient: streamClient,
		}
	case CmdBatchCop:
		var streamClient tikvpb.Tikv_BatchCoprocessorClient
		streamClient, err = client.BatchCoprocessor(ctx, req.BatchCop())
		resp.Resp = &BatchCopStreamResponse{
			Tikv_BatchCoprocessorClient: streamClient,
		}
	case CmdMvccGetByKey:
		kvResp, err1 := client.MvccGetByKey(ctx, req.MvccGetByKey())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdMvccGetByStartTs:
		kvResp, err1 := client.MvccGetByStartTs(ctx, req.MvccGetByStartTs())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdSplitRegion:
		kvResp, err1 := client.SplitRegion(ctx, req.SplitRegion())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdEmpty:
		resp.Resp, err = &tikvpb.BatchCommandsEmptyResponse{}, nil
	case CmdCheckTxnStatus:
		kvResp, err1 := client.KvCheckTxnStatus(ctx, req.CheckTxnStatus())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdCheckSecondaryLocks:
		kvResp, err1 := client.KvCheckSecondaryLocks(ctx, req.CheckSecondaryLocks())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdTxnHeartBeat:
		kvResp, err1 := client.KvTxnHeartBeat(ctx, req.TxnHeartBeat())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdStoreSafeTS:
		kvResp, err1 := client.GetStoreSafeTS(ctx, req.StoreSafeTS())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	case CmdLockWaitInfo:
		kvResp, err1 := client.GetLockWaitInfo(ctx, req.LockWaitInfo())
		resp.Resp = kvResp
		err = err1
		if kvResp != nil && kvResp.Meta != nil {
			resp.ServerSideDuration = time.Duration(kvResp.Meta.ProcessDurationUs) * time.Microsecond
		}

	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return resp, nil
}

// CallDebugRPC launches a debug rpc call.
func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdDebugGetRegionProperties:
		resp.Resp, err = client.GetRegionProperties(ctx, req.DebugGetRegionProperties())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	return resp, err
}

// Lease is used to implement grpc stream timeout.
type Lease struct {
	Cancel   context.CancelFunc
	deadline int64 // A time.UnixNano value, if time.Now().UnixNano() > deadline, cancel() would be called.
}

// Recv overrides the stream client Recv() function.
func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_CoprocessorStreamClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.WithStack(err)
}

// Close closes the CopStreamResponse object.
func (resp *CopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// Recv overrides the stream client Recv() function.
func (resp *BatchCopStreamResponse) Recv() (*coprocessor.BatchResponse, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_BatchCoprocessorClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.WithStack(err)
}

// Close closes the BatchCopStreamResponse object.
func (resp *BatchCopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// Recv overrides the stream client Recv() function.
func (resp *MPPStreamResponse) Recv() (*mpp.MPPDataPacket, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_EstablishMPPConnectionClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.WithStack(err)
}

// Close closes the MPPStreamResponse object.
func (resp *MPPStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// CheckStreamTimeoutLoop runs periodically to check is there any stream request timed out.
// Lease is an object to track stream requests, call this function with "go CheckStreamTimeoutLoop()"
// It is not guaranteed to call every Lease.Cancel() putting into channel when exits.
// If grpc-go supports SetDeadline(https://github.com/grpc/grpc-go/issues/2917), we can stop using this method.
func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{}) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	array := make([]*Lease, 0, 1024)

	for {
		select {
		case <-done:
		drainLoop:
			// Try my best cleaning the channel to make SendRequest which is blocking by it continues.
			for {
				select {
				case <-ch:
				default:
					break drainLoop
				}
			}
			return
		case item := <-ch:
			array = append(array, item)
		case now := <-ticker.C:
			array = keepOnlyActive(array, now.UnixNano())
		}
	}
}

// keepOnlyActive removes completed items, call cancel function for timeout items.
func keepOnlyActive(array []*Lease, now int64) []*Lease {
	idx := 0
	for i := 0; i < len(array); i++ {
		item := array[i]
		deadline := atomic.LoadInt64(&item.deadline)
		if deadline == 0 || deadline > now {
			array[idx] = array[i]
			idx++
		} else {
			item.Cancel()
		}
	}
	return array[:idx]
}

// IsGreenGCRequest checks if the request is used by Green GC's protocol. This is used for failpoints to inject errors
// to specified RPC requests.
func (req *Request) IsGreenGCRequest() bool {
	if req.Type == CmdCheckLockObserver ||
		req.Type == CmdRegisterLockObserver ||
		req.Type == CmdRemoveLockObserver ||
		req.Type == CmdPhysicalScanLock {
		return true
	}
	return false
}

// IsTxnWriteRequest checks if the request is a transactional write request. This is used for failpoints to inject
// errors to specified RPC requests.
func (req *Request) IsTxnWriteRequest() bool {
	if req.Type == CmdPessimisticLock ||
		req.Type == CmdPrewrite ||
		req.Type == CmdCommit ||
		req.Type == CmdBatchRollback ||
		req.Type == CmdPessimisticRollback ||
		req.Type == CmdCheckTxnStatus ||
		req.Type == CmdCheckSecondaryLocks ||
		req.Type == CmdCleanup ||
		req.Type == CmdTxnHeartBeat ||
		req.Type == CmdResolveLock {
		return true
	}
	return false
}

// ResourceGroupTagger is used to fill the ResourceGroupTag in the kvrpcpb.Context.
type ResourceGroupTagger func(req *Request)
