import { CommitmentLevel, SubscribeRequest } from "@triton-one/yellowstone-grpc";
import pino from "pino";
const transport = pino.transport({
  target: 'pino-pretty',
});

export const logger = pino(
  {
    level: 'info',
    serializers: {
      error: pino.stdSerializers.err,
    },
    base: undefined,
  },
  transport,
);


import Client from "@triton-one/yellowstone-grpc";
import { LIQUIDITY_STATE_LAYOUT_V4, LiquidityPoolKeysV4, LiquidityStateLayoutV4, LiquidityStateV4, MARKET_STATE_LAYOUT_V3, Token, TokenAmount ,Liquidity} from "@raydium-io/raydium-sdk";
import { Connection, LAMPORTS_PER_SOL, PublicKey, PublicKeyInitData } from "@solana/web3.js";
import { bufferRing } from "./openbook";
import { buy, sell } from "../transaction/transaction";
import { createPoolKeys } from "../liquidity";
import { MIN_POOL_SIZE, MAX_POOL_SIZE, RPC_ENDPOINT, MIN_POOL_SIZE_SELL, MAX_POOL_SIZE_SELL,RPC_WEBSOCKET_ENDPOINT} from "../constants";
import { PoolFilters, PoolFilterArgs } from "../filters";

function slotExists(slot: number): boolean {
  return true
}

const solanaConnection = new Connection(RPC_ENDPOINT);

let isBought:boolean=false
export async function streamNewTokens(client:Client) {
  const stream = await client.subscribe();
  const sellstream=await client.subscribe()
  let count=0;
  stream.on("data", (data) => {
    
    if (data.account != undefined) {
      let slotCheckResult = true;

      if (slotCheckResult&&isBought==false) {
        isBought=true
        const poolstate = LIQUIDITY_STATE_LAYOUT_V4.decode(data.account.account.data);
        
        if (poolstate.baseMint.toString().slice(-4)!="pump"){
          const tokenAccount = new PublicKey(data.account.account.pubkey);
  
          let attempts = 0;
          const maxAttempts = 2;
  
          const intervalId = setInterval(async () => {
            const marketDetails = bufferRing.findPattern(poolstate.baseMint);
            if (Buffer.isBuffer(marketDetails)) {
              const fullMarketDetailsDecoded = MARKET_STATE_LAYOUT_V3.decode(marketDetails);
              const marketDetailsDecoded = {
                bids: fullMarketDetailsDecoded.bids,
                asks: fullMarketDetailsDecoded.asks,
                eventQueue: fullMarketDetailsDecoded.eventQueue,
              };
              try{
                clearInterval(intervalId); // Stop retrying when a match is found
                const poolkeys=createPoolKeys(tokenAccount, poolstate, marketDetailsDecoded)
                
                  const isFreezable=await checkIfTokenIsFrozen(poolkeys.baseMint,solanaConnection) 
                  const MIN_SIZE=new TokenAmount(Token.WSOL, MIN_POOL_SIZE, false)
                  const MAX_SIZE=new TokenAmount(Token.WSOL, MAX_POOL_SIZE, false)
                  const QUOTE_TOKEN=Token.WSOL
                  let args:PoolFilterArgs={
                    minPoolSize:MIN_SIZE,
                    maxPoolSize:MAX_SIZE,
                    quoteToken:QUOTE_TOKEN
                  }
                  const filter=new PoolFilters(solanaConnection,args );
                  const isValid=await filter.execute(poolkeys);
                  if(!isFreezable&&isValid){                   
                    await buy(solanaConnection,tokenAccount, poolstate, marketDetailsDecoded);
                    const MIN_SIZE_SELL=new TokenAmount(Token.WSOL, MIN_POOL_SIZE_SELL, false)
                    const MAX_SIZE_SELL=new TokenAmount(Token.WSOL, MAX_POOL_SIZE_SELL, false)
                    const QUOTE_TOKEN=Token.WSOL
                    let args_sell:PoolFilterArgs={
                      minPoolSize:MIN_SIZE_SELL,
                      maxPoolSize:MAX_SIZE_SELL,
                      quoteToken:QUOTE_TOKEN
                    }
                    const filter_sell=new PoolFilters(solanaConnection,args_sell)
                    
                    while(true){
                      const isValid_sell=await filter_sell.execute(poolkeys)
                      if(isValid_sell){
                        await sell(2,tokenAccount, poolstate, marketDetailsDecoded)
                        break;
                      }
                    }
                    isBought=false
                    // await sell(2,tokenAccount, poolstate, marketDetailsDecoded)
                    
                    const balance_response=await solanaConnection.getTokenAccountBalance(poolkeys.quoteVault);
                    const initial_size=Number(balance_response.value.amount)/LAMPORTS_PER_SOL;
                    logger.info(`Initial pool size-> ${Number(initial_size.toFixed(2))}`);
                    let c=0;
                    let inactive_rate=0;
                    if(initial_size>=170&&initial_size<=200){
                      while(true){
                        await new Promise<void>((resolve)=>setTimeout(resolve,400))
                        const response=await solanaConnection.getTokenAccountBalance(poolkeys.quoteVault);
                        const current_size=Number(response.value.amount)/LAMPORTS_PER_SOL;
                        const diff=current_size-initial_size
                        logger.info( `${diff}`)
                        if(diff>=-5&&diff<=5){
                          inactive_rate++;
                        }
                        if(inactive_rate==80){
                          logger.info("Sell with 0% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                        if(diff<=-20){
                          logger.info("Sell with -10% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }else if(diff>=30&&diff<=50){
                          logger.info("Sell with 15% profit")
                          if(c==0){
                            await sell(1,tokenAccount, poolstate, marketDetailsDecoded);
                            await new Promise<void>((resolve)=>setTimeout(resolve,2000));
                            c++;
                          }else if(c==1){
                            await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                            break;
                          }

                        }else if(diff>=50&&diff<=65){
                          logger.info("Sell with 30% profit")
                          if(c==0){
                            c++;
                            await sell(0,tokenAccount, poolstate, marketDetailsDecoded);
                            await new Promise<void>((resolve)=>setTimeout(resolve,2000))
                          }else if(c==1){
                            await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                            break;
                          }
                        }else if(diff>=50){
                          logger.info("Sell with 50% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                      }
                    }else if(initial_size>=200&&initial_size<=300){
                      while(true){
                        await new Promise<void>((resolve)=>setTimeout(resolve,400))
                        const response=await solanaConnection.getTokenAccountBalance(poolkeys.quoteVault);
                        const current_size=Number(response.value.amount)/LAMPORTS_PER_SOL;
                        const diff=current_size-initial_size
                        logger.info( `${diff}`)
                        if(diff>=-10&&diff<=10){
                          inactive_rate++;
                        }
                        if(inactive_rate==80){
                          logger.info("Sell with 0% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                        if(diff<=-50){
                          logger.info("Sell with -10% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }else if(diff>=50&&diff<=60){
                          logger.info("Sell with 15% profit")
                          if(c==0){
                            c++;
                            await sell(1,tokenAccount, poolstate, marketDetailsDecoded);
                            await new Promise<void>((resolve)=>setTimeout(resolve,2000))
                          }else if(c==1){
                            await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                            break;
                          }
                        }else if(diff>=80&&diff<=120){
                          logger.info("Sell with 30% profit")
                          if(c==0){
                            c++;
                            await sell(0,tokenAccount, poolstate, marketDetailsDecoded);
                            await new Promise<void>((resolve)=>setTimeout(resolve,2000))
                          }else if(c==1){
                            await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                            break;
                          }
                        }else if(diff>=150){
                          logger.info("Sell with 50% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                      }
                    }else if(initial_size>=300&&initial_size<=500){
                      while(true){
                        await new Promise<void>((resolve)=>setTimeout(resolve,400))
                        const response=await solanaConnection.getTokenAccountBalance(poolkeys.quoteVault);
                        const current_size=Number(response.value.amount)/LAMPORTS_PER_SOL;
                        const diff=current_size-initial_size
                        logger.info( `${diff}`)
                        if(diff>=-20&&diff<=20){
                          inactive_rate++;
                        }
                        if(inactive_rate==80){
                          logger.info("Sell with 0% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                        if(diff<=-100){
                          logger.info("Sell with -10% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }else if(diff>=60&&diff<=70){
                          logger.info("Sell with 15% profit")
                          if(c==0){
                            c++;
                            await sell(1,tokenAccount, poolstate, marketDetailsDecoded);
                            await new Promise<void>((resolve)=>setTimeout(resolve,2000))
                          }else if(c==1){
                            await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                            break;
                          }
                        }else if(diff>=80&&diff<=150){
                          logger.info("Sell with 30% profit")
                          if(c==0){
                            c++;
                            await sell(0,tokenAccount, poolstate, marketDetailsDecoded);
                            await new Promise<void>((resolve)=>setTimeout(resolve,2000))
                          }else if(c==1){
                            await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                            break;
                          }
                        }else if(diff>=200){
                          logger.info("Sell with 50% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                      }
                    }else if(initial_size>500||initial_size<170){
                      while(true){
                        await new Promise<void>((resolve)=>setTimeout(resolve,400))
                        const response=await solanaConnection.getTokenAccountBalance(poolkeys.quoteVault);
                        const current_size=Number(response.value.amount)/LAMPORTS_PER_SOL;
                        const diff=current_size-initial_size
                        logger.info( `${diff}`)
                        if(diff<=-30||diff>=0){
                          logger.info("Sell with 0% profit")
                          await sell(2,tokenAccount, poolstate, marketDetailsDecoded);
                          break;
                        }
                      }
                    }
                    
                    
                      

                    //   let balance_response=await solanaConnection.getTokenAccountBalance(poolkeys.quoteVault);
                    //   if(Number(balance_response.value.uiAmount)<=150){
                    //     await sell(tokenAccount, poolstate, marketDetailsDecoded)
                    //     break;
                    //   }
                    //   if(Number(balance_response.value.amount)<=10){
                    //     break;
                    //   }

                    //   await new Promise<void>((resolve)=>setTimeout(resolve,500))
                    // }
                  }else{
                    logger.info(`Token ${poolkeys.baseMint} can be frozen.`)
                    isBought=false
                  }
                // }
              }catch(e){
                logger.error(e);
                isBought=false;
              }
            } else if (attempts >= maxAttempts) {
              logger.error("Invalid market details");
              clearInterval(intervalId); 
              isBought=false
            }
            attempts++;
          }, 100);
        }
        
      }
    }
  });

  // Create a subscription request.
  const request: SubscribeRequest = {
    "slots": {},
    "accounts": {
      "raydium": {
        "account": [],
        "filters": [
          {datasize:LIQUIDITY_STATE_LAYOUT_V4.span.toString()},
          {
            "memcmp": {
              "offset": LIQUIDITY_STATE_LAYOUT_V4.offsetOf('quoteMint').toString(), // Filter for only tokens paired with SOL
              "base58": "So11111111111111111111111111111111111111112"
            }
          },
          {
            "memcmp": {
              "offset": LIQUIDITY_STATE_LAYOUT_V4.offsetOf('swapQuoteInAmount').toString(), // Hack to filter for only new tokens. There is probably a better way to do this
              "bytes": Uint8Array.from([0])
            }
          },
          {
            "memcmp": {
              "offset": LIQUIDITY_STATE_LAYOUT_V4.offsetOf('swapBaseOutAmount').toString(), // Hack to filter for only new tokens. There is probably a better way to do this
              "bytes": Uint8Array.from([0])
            }
          }
          
        ],
        "owner": ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"] // raydium program id to subscribe to
      }
    },
    "transactions": {},
    "blocks": {},
    "blocksMeta": {},
    "accountsDataSlice": [],
    "commitment": CommitmentLevel.PROCESSED,  // Subscribe to processed blocks for the fastest updates
    entry: {}
  }
  
  // Sending a subscription request.
  await new Promise<void>((resolve, reject) => {
    stream.write(request, (err: null | undefined) => {
      if (err === null || err === undefined) {
        resolve();
      } else {
        reject(err);
      }
    });
  }).catch((reason) => {
    throw reason;
  });
}

async function checkIfTokenIsFrozen(mintAddress:PublicKey, connection:Connection) {
  const mintInfo = await connection.getParsedAccountInfo(mintAddress);
  
  if (mintInfo.value) {
      const accountData = mintInfo.value.data;

      // Check if accountData is of type ParsedAccountData
      if ('parsed' in accountData) {
          const freezeAuthority = accountData.parsed.info.freezeAuthority;
          if (freezeAuthority) {
              return true
          } else {
              return false
          }
      } else {
          console.log('Account data is not parsed or is a Buffer.');
      }
  } else {
      console.log('Mint account not found.');
  }
}