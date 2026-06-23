// deploy.ts - load-tester contract deployer.
//
// Runs inside the uniswap-v3-core hardhat project (it has the contracts,
// typechain and the `stable` network). Deploys: UniswapV3Factory, two
// TestERC20 tokens, one pool (initialized + seeded with liquidity),
// TestUniswapV3Callee, and Destructible. Emits deployment.json consumed by the
// Go harness.
//
// Prereqs:
//   1. cp load-tester/ts/Destructible.sol <uniswap-v3-core>/contracts/test/
//   2. from <uniswap-v3-core>: npx hardhat compile
//
// Run (from <uniswap-v3-core>):
//   STABLE_RPC_URL=http://127.0.0.1:8545 STABLE_CHAIN_ID=999 \
//   LT_DEPLOYMENT_OUT=/abs/path/stable/load-tester/deployment.json \
//   npx hardhat run /abs/path/stable/load-tester/ts/deploy.ts --network stable

// NOTE: run from INSIDE the uniswap-v3-core project so 'hardhat'/'ethers'
// resolve against its node_modules (ethers v5). Copy this file into the
// project's scripts/ dir (see README) before `npx hardhat run`.
import { ethers } from "hardhat";
import { BigNumber } from "ethers";
import * as fs from "fs";

const FEE = 3000;
const TICK_LOWER = -887220; // multiple of tickSpacing(60) for fee 3000
const TICK_UPPER = 887220;
const SQRT_PRICE_1_1 = BigNumber.from("79228162514264337593543950336"); // 2^96
const LIQUIDITY = BigNumber.from("1000000000000000000"); // 1e18

async function main() {
  const [deployer] = await ethers.getSigners();
  console.log("deployer:", deployer.address);

  // 1. Factory
  const Factory = await ethers.getContractFactory("UniswapV3Factory");
  const factory = await Factory.deploy();
  await factory.deployed();
  console.log("factory:", factory.address);

  // 2. Two TestERC20 tokens (large supply on the deployer).
  const supply = BigNumber.from(2).pow(255);
  const ERC20 = await ethers.getContractFactory("TestERC20");
  const tA = await (await ERC20.deploy(supply)).deployed();
  const tB = await (await ERC20.deploy(supply)).deployed();
  // sort so token0 < token1
  let token0 = tA, token1 = tB;
  if (tA.address.toLowerCase() > tB.address.toLowerCase()) {
    token0 = tB; token1 = tA;
  }
  console.log("token0:", token0.address, "token1:", token1.address);

  // 3. Pool: create + initialize.
  await (await factory.createPool(token0.address, token1.address, FEE)).wait();
  const poolAddr = await factory.getPool(token0.address, token1.address, FEE);
  const pool = await ethers.getContractAt("UniswapV3Pool", poolAddr);
  await (await pool.initialize(SQRT_PRICE_1_1)).wait();
  console.log("pool:", poolAddr);

  // 4. Callee (swap/mint router).
  const Callee = await ethers.getContractFactory("TestUniswapV3Callee");
  const callee = await (await Callee.deploy()).deployed();
  console.log("callee:", callee.address);

  // 5. Seed liquidity: approve callee, mint a full-range position.
  await (await token0.approve(callee.address, ethers.constants.MaxUint256)).wait();
  await (await token1.approve(callee.address, ethers.constants.MaxUint256)).wait();
  await (await callee.mint(poolAddr, deployer.address, TICK_LOWER, TICK_UPPER, LIQUIDITY)).wait();
  console.log("liquidity seeded");

  // 6. Destructible.
  const Destructible = await ethers.getContractFactory("Destructible");
  const destructible = await (await Destructible.deploy()).deployed();
  console.log("destructible:", destructible.address);

  const deployment = {
    factory: factory.address,
    callee: callee.address,
    gasToken: process.env.LT_GAS_TOKEN || "",
    destructible: destructible.address,
    tokens: [
      { symbol: "T0", address: token0.address },
      { symbol: "T1", address: token1.address },
    ],
    pools: [
      { address: poolAddr, token0: token0.address, token1: token1.address, fee: FEE },
    ],
  };

  const out = process.env.LT_DEPLOYMENT_OUT || "deployment.json";
  fs.writeFileSync(out, JSON.stringify(deployment, null, 2));
  console.log("wrote", out);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
