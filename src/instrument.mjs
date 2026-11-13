import net from 'node:net';
export async function attach(state){
  const sockets=new Set();
  const server=net.createServer(socket=>{
    sockets.add(socket);socket.on('close',()=>sockets.delete(socket));let pending='';let chain=Promise.resolve();
    async function send(data){for(let offset=0;offset<data.length;offset+=state.fragment){if(socket.destroyed)return;socket.write(data.subarray(offset,offset+state.fragment));await new Promise(resolve=>setTimeout(resolve,1));}}
    async function command(line){
      if(state.notify)await send(Buffer.from(`!STATUS ${state.generation}\n`));
      if(line==='*IDN?')return send(Buffer.from('LOCAL,MEASURE,17,1\n'));
      if(line.startsWith('CONF:UNIT ')){const unit=line.slice(10);if(!['V','MV'].includes(unit))return send(Buffer.from('ERR UNIT\n'));state.unit=unit;return send(Buffer.from('OK\n'));}
      if(line==='INIT'){state.trigger++;state.measuredUnit=state.unit;return send(Buffer.from(`OK ${state.trigger}\n`));}
      if(line==='STAT?')return send(Buffer.from(JSON.stringify({generation:state.generation,trigger:state.trigger,unit:state.measuredUnit})+'\n'));
      if(line==='FETCH?'){const values=Buffer.alloc(16);values.writeDoubleBE(state.measuredUnit==='MV'?1000:1,0);values.writeDoubleBE(2,8);values[7]=10;const block=Buffer.concat([Buffer.from('#216'),values,Buffer.from('\n')]);if(state.truncate){await send(block.subarray(0,9));return socket.end();}return send(block);}
      return send(Buffer.from('ERR COMMAND\n'));
    }
    socket.on('data',chunk=>{pending+=chunk.toString();let boundary;while((boundary=pending.indexOf('\n'))>=0){const line=pending.slice(0,boundary).trim();pending=pending.slice(boundary+1);chain=chain.then(()=>command(line)).catch(()=>socket.destroy());}});socket.on('error',()=>{});
  });await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  return {endpoint:{host:'127.0.0.1',port:server.address().port},close:async()=>{for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));}};
}
export async function identify(endpoint){return new Promise((resolve,reject)=>{const socket=net.connect(endpoint.port,endpoint.host);let text='';socket.setTimeout(1500,()=>socket.destroy(new Error('仪器识别超时')));socket.on('connect',()=>socket.write('*IDN?\n'));socket.on('data',chunk=>{text+=chunk.toString();if(text.includes('\n')){socket.end();resolve(text.trim());}});socket.on('error',reject);});}
