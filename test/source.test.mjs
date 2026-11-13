import test from 'node:test';
import assert from 'node:assert/strict';
import {createSource} from '../src/source.mjs';
import {inspect,readRaw} from '../src/client.mjs';

async function withSource(callback){const source=await createSource();try{return await callback(source);}finally{await source.close();}}
function access(source){return async(path,options={})=>{const response=await fetch(source.url+path,{method:options.method||'GET',body:options.body===undefined?undefined:JSON.stringify(options.body),signal:AbortSignal.timeout(4000)});const text=await response.text();let value=text;try{value=JSON.parse(text);}catch{}return {status:response.status,value};};}
test('来源存活与单次目录读取',()=>withSource(async source=>{const api=access(source);assert.equal((await api('/health')).value.status,'ok');assert.ok(Object.keys(await inspect(source.url)).length>0);assert.ok((await readRaw(source.url,'/inventory')).length>0);}));
test('未知路径与坏控制请求明确拒绝',()=>withSource(async source=>{const api=access(source);assert.equal((await api('/missing')).status,404);const invalid=await fetch(source.url+'/control',{method:'POST',body:'not-json'});assert.equal(invalid.status,400);}));
test('来源领域契约的状态变化',()=>withSource(async source=>{const api=access(source);const {identify}=await import('../src/instrument.mjs');assert.equal(await identify((await api('/state')).value.endpoint),'LOCAL,MEASURE,17,1');assert.equal((await api('/state')).value.trigger,0);}));


test('仪器分块响应保留二进制内部换行',()=>withSource(async source=>{
  const net=await import('node:net');const {endpoint}=source.state;
  const data=await new Promise((resolve,reject)=>{const socket=net.connect(endpoint.port,endpoint.host);let bytes=Buffer.alloc(0);socket.setTimeout(2000,()=>socket.destroy(new Error('块响应超时')));socket.on('connect',()=>socket.write('INIT\nFETCH?\n'));socket.on('data',chunk=>{bytes=Buffer.concat([bytes,chunk]);if(bytes.length>=26){socket.end();resolve(bytes);}});socket.on('error',reject);});
  assert.equal(data.subarray(0,5).toString(),'OK 1\n');assert.equal(data.subarray(5,9).toString(),'#216');assert.equal(data.subarray(9,25).length,16);assert.ok(data.subarray(9,25).includes(10));
}));
