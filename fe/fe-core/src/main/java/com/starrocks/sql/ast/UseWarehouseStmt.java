// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/sql/ast/UseWarehouseStmt.java
public class UseWarehouseStmt extends StatementBase {
    private final String warehouseName;

    public UseWarehouseStmt(String warehouseName) {
        this.warehouseName = warehouseName;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUseWarehouseStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

=======
#include "common/statusor.h"
#include "exec/spill/spiller.h"

namespace starrocks {
namespace spill {
std::shared_ptr<Spiller> SpillerFactory::create(const SpilledOptions& options) {
    std::lock_guard guard(_mutex);
    auto spiller = std::make_shared<Spiller>(options, shared_from_this());
    _spillers.emplace_back(spiller);
    return spiller;
}

SpillerFactoryPtr make_spilled_factory() {
    return std::make_shared<SpillerFactory>();
}
} // namespace spill
} // namespace starrocks
>>>>>>> e8b0953df ([Enhancement][Refactor] Reduce the number of spill files (#18828)):be/src/exec/spill/spiller_factory.cpp
