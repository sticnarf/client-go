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

package oracles_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"testing"
)

type mockCountedOracle struct {
	oracle.Oracle
	count int
}

func (o *mockCountedOracle) GetTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	o.count++
	return o.Oracle.GetTimestamp(ctx, opt)
}

func TestRoundRobinStrategy(t *testing.T) {
	var list []oracle.Oracle
	for i := 0; i < 4; i++ {
		list = append(list, &mockCountedOracle{oracles.NewLocalOracle(), 0})
	}
	o := oracles.NewMultiOracles(list)
	for i := 0; i < 3*4; i++ {
		_, _ = o.GetTimestamp(context.Background(), &oracle.Option{})
	}
	for _, o := range list {
		assert.Equal(t, 3, o.(*mockCountedOracle).count)
	}
}
