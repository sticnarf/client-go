// Copyright 2022 TiKV Authors
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

package oracles

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/tikv/client-go/v2/oracle"
)

// roundRobinOracle is a timestamp oracle that includes multiple upstream oracles.
// The upstream oracle is chosen with a round-robin strategy.
type roundRobinOracle struct {
	oracles []oracle.Oracle
	// the index of the last used oracle
	index uint32
}

func (m *roundRobinOracle) getOracle() oracle.Oracle {
	index := int(atomic.AddUint32(&m.index, 1)) % len(m.oracles)
	return m.oracles[index]
}

func (m *roundRobinOracle) GetTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	return m.getOracle().GetTimestamp(ctx, opt)
}

func (m *roundRobinOracle) GetTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	return m.getOracle().GetTimestampAsync(ctx, opt)
}

func (m *roundRobinOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	return m.getOracle().GetLowResolutionTimestamp(ctx, opt)
}

func (m *roundRobinOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	return m.getOracle().GetLowResolutionTimestampAsync(ctx, opt)
}

func (m *roundRobinOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (uint64, error) {
	return m.getOracle().GetStaleTimestamp(ctx, txnScope, prevSecond)
}

func (m *roundRobinOracle) IsExpired(lockTimestamp, TTL uint64, opt *oracle.Option) bool {
	return m.getOracle().IsExpired(lockTimestamp, TTL, opt)
}

func (m *roundRobinOracle) UntilExpired(lockTimeStamp, TTL uint64, opt *oracle.Option) int64 {
	return m.getOracle().UntilExpired(lockTimeStamp, TTL, opt)
}

func (m *roundRobinOracle) Close() {
	for _, o := range m.oracles {
		o.Close()
	}
}

func NewMultiOracles(oracles []oracle.Oracle) oracle.Oracle {
	return &roundRobinOracle{
		oracles: oracles,
		index:   math.MaxUint32,
	}
}
