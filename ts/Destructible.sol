// SPDX-License-Identifier: UNLICENSED
pragma solidity =0.7.6;

// Destructible exercises BlockSTM/MemIAVL determinism edge cases.
//
//   - bump(): writes a single shared storage slot. Many accounts calling bump()
//     in the same block create write-write contention that stresses BlockSTM's
//     conflict detection / re-execution path.
//   - spawnAndDestroy(count): CREATEs `count` child contracts that SELFDESTRUCT
//     inside the same transaction, exercising same-block create+selfdestruct.
//
// Copy this into uniswap-v3-core/contracts/test/ before running deploy.ts so it
// compiles under the same solidity 0.7.6 toolchain.
contract Destructible {
    uint256 public counter;

    event Bumped(uint256 newValue);
    event Spawned(uint256 count);

    // bump writes the shared counter slot (same-slot contention across senders).
    function bump() external {
        counter = counter + 1;
        emit Bumped(counter);
    }

    // spawnAndDestroy deploys `count` children that selfdestruct on construction.
    function spawnAndDestroy(uint256 count) external {
        for (uint256 i = 0; i < count; i++) {
            new Child();
        }
        emit Spawned(count);
    }
}

// Child selfdestructs immediately in its constructor: a create+selfdestruct in
// the same transaction (and same block).
contract Child {
    constructor() {
        selfdestruct(msg.sender);
    }
}
