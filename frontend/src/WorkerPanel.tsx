import {useEffect,useState} from 'react';

type Props={jobId:string,api:(path:string,body?:unknown)=>Promise<any>,onJob:(job:any)=>void};
export function WorkerPanel({jobId,api,onJob}:Props){
 const [data,D]=useState<any>(),[error,E]=useState(''),[busy,B]=useState(false);
 async function refresh(){const result=await api(`/replay/jobs/${jobId}/dashboard`);D(result);onJob(result.job)}
 useEffect(()=>{D(undefined);E('');refresh().catch(e=>E(String(e)));const timer=setInterval(()=>refresh().catch(e=>E(String(e))),2000);return()=>clearInterval(timer)},[jobId]);
 async function command(name:string){B(true);E('');try{await api(`/replay/jobs/${jobId}/${name}`,{});await refresh()}catch(e){E(String(e))}finally{B(false)}}
 if(!data)return <p>{error||'Loading committed progress…'}</p>;
 const state=data.job.state;
 return <div className="execution"><h3>Execution · {state}</h3>{error&&<p className="error" role="alert">{error}</p>}{data.statusDetail&&<p role="status" className="error">{data.statusDetail}</p>}
 {!data.job.workerEnabled&&<aside>Live execution is disabled. Configure the worker and receipt topic before starting.</aside>}
 <div className="actions">
 {state==='APPROVED'&&<button disabled={busy||!data.job.workerEnabled} onClick={()=>command('start')}>{data.job.canary?'Start one-record canary':'Start approved replay'}</button>}
 {state==='AWAITING_RELEASE'&&<button disabled={busy} onClick={()=>command('release')}>Release remaining approved records</button>}
 {['RUNNING','CANARY_RUNNING'].includes(state)&&<button disabled={busy} onClick={()=>command('pause')}>Pause at batch boundary</button>}
 {['PAUSED','BLOCKED'].includes(state)&&<button disabled={busy} onClick={()=>command('resume')}>Reauthorize, reconcile and resume</button>}
 <button disabled={busy} onClick={()=>refresh().catch(e=>E(String(e)))}>Refresh progress</button></div>
 <div className="grid"><div><strong>{data.published}</strong><br/>Committed outputs</div><div><strong>{data.skipped}</strong><br/>Explicit skipped outcomes</div><div><strong>{data.processed} / {data.job.count}</strong><br/>Manifest positions processed</div><div><strong>{data.batches}</strong><br/>Committed batch receipts</div></div>
 <p>{data.source}. Progress observed: {data.progressObservedAt?new Date(data.progressObservedAt).toLocaleString():'no committed receipts yet'}.</p>
 <p>Mean source record age: {Math.round(data.meanRecordAgeMs/1000)} seconds. {data.ageSource}.</p>
 <p>Time from approval to first output: {data.timeToFirstReplayMs===null?'pending':`${data.timeToFirstReplayMs} ms`}. Publish rate: {Number(data.publishRatePerSecond).toFixed(1)} records/s. {data.rateSource}.</p>
 <p>Group lag: {data.groupLag.available?JSON.stringify(data.groupLag.partitions):data.groupLag.reason}. Source: {data.groupLag.source}; sampled {new Date(data.groupLag.observedAt).toLocaleString()}.</p>
 <details><summary>Receipt and source-to-destination audit ({data.receipts.length} batches)</summary><p>Recovery stable boundary: {data.recoveryBoundary??'not captured'}. Read-committed receipt outcomes determine these counts.</p><div className="receipt-list"><table><thead><tr><th>Manifest / source</th><th>Outcome / destination</th><th>Committed receipt</th></tr></thead><tbody>{data.receipts.flatMap((r:any)=>r.receipt.outcomes.map((o:any)=><tr key={o.position}><td>#{o.position} · {o.sourceTopic} P{o.sourcePartition}@{o.sourceOffset}</td><td>{o.status}{o.destinationOffset!==null&&<><br/>{o.destinationTopic} P{o.destinationPartition}@{o.destinationOffset}</>}</td><td>{r.receiptTopic} P0@{r.receiptOffset}<br/>Batch {r.receipt.batchIndex}</td></tr>))}</tbody></table></div><pre>{JSON.stringify({events:data.events,commands:data.commands},null,2)}</pre></details>
 {state==='COMPLETED'&&<aside>All approved records have committed output receipts. Downstream business processing is separate.</aside>}
 {state==='COMPLETED_WITH_EXCEPTIONS'&&<aside>Execution finished with skipped source records. Review every skipped receipt outcome.</aside>}
 </div>;
}
