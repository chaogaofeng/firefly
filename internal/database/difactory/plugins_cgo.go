// Copyright © 2021 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
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

//go:build cgo
// +build cgo

package difactory

import (
	"github.com/hyperledger/firefly/internal/database/postgres"
	"github.com/hyperledger/firefly/internal/database/sqlite3"
	"github.com/hyperledger/firefly/pkg/database"
)

var plugins = []database.Plugin{
	&postgres.Postgres{},
	&sqlite3.SQLite3{}, // wrapper to the SQLite 3 C library
}
