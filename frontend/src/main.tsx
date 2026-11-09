import React, {useEffect,useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';
import {Help} from './Help';
import {PlannerPanel} from './PlannerPanel';
import {RecordsTable, type RecordRow} from './RecordsTable';
type Profile={id:string,name:string,bootstrap:string,clusterId:string};
type Page={records:RecordRow[],cursors:Record<string,number>,beginnings:Record<string,number>,boundaries:Record<string,number>,scanned:number,truncated:boolean};
const emptyConnection={name:'',bootstrap:'',protocol:'SASL_SSL',username:'',password:'',caPem:'',replayUsername:'',replayPassword:''};
const miniKafkaChoice='mini-kafka-preset';
const newConnectionChoice='new-connection';

function App(){
 const [tab,setTab]=useState('connection');
 const [theme,setTheme]=useState(()=>localStorage.getItem('replay-theme')||'dark');
 useEffect(()=>{document.documentElement.dataset.theme=theme;localStorage.setItem('replay-theme',theme)},[theme]);

 const [session,S]=useState<{mode:string,user:string,roles:string[],csrfToken:string}>(),[profiles,P]=useState<Profile[]>([]),[choice,C]=useState(''),[topics,T]=useState<string[]>([]),[topic,D]=useState(''),[page,B]=useState<Page>(),[error,E]=useState(''),[busy,U]=useState(false);
 const [form,F]=useState(emptyConnection),[tested,X]=useState<{testToken:string,clusterId:string}>();
 const [loadingTopics,setLoadingTopics]=useState(false);
 const isLab=session?.mode==='lab';
 const canConnect=(isLab||session?.mode==='live')&&session.roles.includes('Admin');
 const miniKafka=choice===miniKafkaChoice;
 const showConnectionForm=canConnect&&(miniKafka||choice===newConnectionChoice);
 const selected=profiles.some(profile=>profile.id===choice)?choice:'';
 const [filters,Q]=useState({partition:'',from:'',to:'',key:'',exception:'',header:'',limit:'25',scanLimit:'1000'});
 async function api(path:string,body?:unknown){const r=await fetch('/api'+path,{method:body===undefined?'GET':'POST',headers:{'Content-Type':'application/json','X-XSRF-TOKEN':session?.csrfToken||''},body:body===undefined?undefined:JSON.stringify(body)});if(!r.ok){let data;try{data=await r.json()}catch{}throw Error(data?.error||`Request failed (${r.status})`)}return r.json();}
 async function run(fn:()=>Promise<void>){E('');U(true);try{await fn()}catch(e){E(String(e))}finally{U(false)}}
 useEffect(()=>{run(async()=>{
  const current=await api('/session');
  S(current);
  const saved:Profile[]=await api('/profiles');
  P(saved);
  if(saved.length)C(saved[0].id);
  else if(current.mode==='live'&&current.roles.includes('Admin'))C(newConnectionChoice);
 })},[]);
 useEffect(()=>{
  T([]);
  D('');
  B(undefined);
  setLoadingTopics(Boolean(selected));
  if(!selected)return;
  let current=true;
  api(`/profiles/${selected}/topics`)
   .then((available:string[])=>{
    if(!current)return;
    T(available);
    D(available[0]||'');
   })
   .catch(error=>{if(current)E(String(error))})
   .finally(()=>{if(current)setLoadingTopics(false)});
  return ()=>{current=false};
 },[selected]);

 function chooseConnection(next:string){
  C(next);
  T([]);
  D('');
  B(undefined);
  X(undefined);
  E('');
  if(next===miniKafkaChoice){
   F({...emptyConnection,name:'Mini Kafka',bootstrap:'localhost:19092',protocol:'PLAINTEXT'});
  }else{
   F({...emptyConnection,protocol:isLab?'PLAINTEXT':'SASL_SSL'});
  }
 }
 function query(cursors?:Record<string,number>){return {...filters,partition:filters.partition===''?null:Number(filters.partition),from:filters.from?new Date(filters.from).getTime():null,to:filters.to?new Date(filters.to).getTime():null,limit:Number(filters.limit),scanLimit:Number(filters.scanLimit),cursors};}
 async function browse(next=false){B(await api(`/profiles/${selected}/topics/${encodeURIComponent(topic)}/browse`,query(next?page?.cursors:undefined)))}
 async function revealRecord(record:RecordRow) {
  const result:Page=await api(`/profiles/${selected}/topics/${encodeURIComponent(topic)}/reveal`,{
   partition:record.partition,cursors:{[record.partition]:record.offset},limit:1,scanLimit:1
  });
  const revealed=result.records.find(row=>row.topic===record.topic&&row.partition===record.partition&&row.offset===record.offset);
  if(!revealed)throw Error('This record is no longer available at its original offset. Browse records again.');
  B(current=>current&&{...current,records:current.records.map(row=>row===record?revealed:row)});
 }
 return <><header><span className="logo">↻</span><div><h1>Kafka Dev Tool</h1><small>EVENT RECOVERY TERMINAL / v0.3</small></div><span className="badge">{selected==='demo'?'SIMULATION':selected==='kafka-local'?'KAFKA DEMO':session?.mode==='live'?'LIVE':'CONSOLE'}</span><span>{session?.user}</span><button className="theme-toggle" title="Toggle dark / light theme" aria-label="Toggle dark / light theme" onClick={()=>setTheme(theme==='dark'?'light':'dark')}>{theme==='dark'?'☾':'☀'}</button></header><div className="workspace"><aside className="sidebar"> <nav className="tabs" aria-orientation="vertical" role="tablist" aria-label="Console navigation">{['connection','records',...(session?.roles.some(r=>r==='Admin'||r==='Operator')?['replay','jobs']:[])].map((name,i)=><button key={name} id={'tab-'+name} role="tab" aria-selected={tab===name} aria-controls={'panel-'+name} onKeyDown={e=>{if(['ArrowDown','ArrowUp','ArrowRight','ArrowLeft'].includes(e.key)){e.preventDefault();const buttons=Array.from(e.currentTarget.parentElement!.querySelectorAll<HTMLButtonElement>('[role="tab"]'));const next=buttons[(i+(e.key==='ArrowRight'||e.key==='ArrowDown'?1:buttons.length-1))%buttons.length];next.click();next.focus()}}} onClick={()=>setTab(name)}><span>0{i+1}</span> {name.toUpperCase()}</button>)}</nav></aside><main>

 {error&&<p role="alert" className="error">{error} {!session&&<a href="/oauth2/authorization/console">Sign in to the live console</a>}</p>}

<div hidden={tab!=='connection'} role="tabpanel" id="panel-connection" aria-labelledby="tab-connection">
 <section>
  <h2>Connection</h2>
  <section className="toolbar">
   <label>Cluster *
    <select required value={choice} disabled={busy} onChange={e=>chooseConnection(e.target.value)}>
     <option value="">Choose a cluster</option>
     {profiles.some(profile=>profile.id==='demo')&&<option value="demo">Built-in mock</option>}
     {canConnect&&isLab&&<option value={miniKafkaChoice}>Mini Kafka implementation</option>}
     {canConnect&&<option value={newConnectionChoice}>New connection</option>}
     {profiles.filter(profile=>profile.id!=='demo').map(profile=>(
      <option key={profile.id} value={profile.id}>{profile.name} · {profile.bootstrap}</option>
     ))}
    </select>
   </label>
  </section>
  {selected==='demo'&&<p>Browse sample failed events and simulate replays without starting a broker.</p>}
  {selected&&<button disabled={busy||loadingTopics} onClick={()=>setTab('records')}>Continue to records →</button>}
 </section>
 {showConnectionForm&&<section>
  <h2>Connect a cluster</h2>
  {miniKafka&&<p>From the repository root in a second terminal, run <code>./scripts/start-mini-kafka.ps1</code>. When Mini Kafka is running, test and save the connection below.</p>}
  <Help text="Enter Kafka host:port endpoints, separated by commas. HTTP dashboard URLs are not accepted."/>
  <div className="grid">
   {Object.entries(form).filter(([key])=>!isLab||['name','bootstrap','protocol'].includes(key)).map(([key,value])=>{
    const required=['name','bootstrap','protocol'].includes(key)||(form.protocol==='SASL_SSL'&&['username','password','replayUsername','replayPassword'].includes(key));
    return <label key={key}>{key}{required?' *':''}
     {key==='protocol'
      ?<select value={value} disabled={busy} onChange={e=>{F({...form,protocol:e.target.value});X(undefined)}}>
        {!isLab&&<option>SASL_SSL</option>}
        <option>PLAINTEXT</option>
       </select>
      :<input required={required} value={value} disabled={busy}
        type={key.toLowerCase().includes('password')?'password':'text'} autoComplete="off"
        placeholder={key==='bootstrap'?'localhost:9092 or broker-a.example.org:9093,broker-b.example.org:9093':''}
        onChange={e=>{F({...form,[key]:e.target.value});X(undefined)}}/>
     }
    </label>;
   })}
  </div>
  <button disabled={busy} onClick={()=>run(async()=>X(await api('/profiles/test',form)))}>Test connection</button>{' '}
  <Help text="Validate broker connectivity and credentials before saving this cluster."/>
  {tested&&<>
   <p>Connected: {tested.clusterId} <Help text="Connection test expires in five minutes."/></p>
   <button disabled={busy} onClick={()=>run(async()=>{
    const profile:Profile=await api('/profiles',{testToken:tested.testToken});
    P(await api('/profiles'));
    chooseConnection(profile.id);
    setTab('records');
   })}>Save and open</button>{' '}
   <Help text="Save the tested profile and open its topics."/>
  </>}
 </section>}
</div>
 <div hidden={tab!=='records'} role="tabpanel" id="panel-records" aria-labelledby="tab-records"><section><h2>Dead-letter records <Help text="Browse failed events without publishing. Filters also apply to your replay preview."/></h2><div className="grid"><label>Topic *<select required value={topic} onChange={e=>{D(e.target.value);B(undefined)}}>{topics.map(t=><option key={t}>{t}</option>)}</select></label>{Object.entries(filters).map(([k,v])=><label key={k}>{k==='key'?'Key · base64':k}{['limit','scanLimit'].includes(k)?' *':''}<input required={['limit','scanLimit'].includes(k)} type={k==='from'||k==='to'?'datetime-local':'text'} value={v} onChange={e=>Q({...filters,[k]:e.target.value})}/></label>)}</div><button disabled={busy||!topic} onClick={()=>run(()=>browse())}>{busy?'Reading…':'Browse records'}</button> <Help text="Read a bounded scan using the current filters. This does not publish records."/>
 {page&&<><p>{page.records.length} matches · {page.scanned} records scanned · {page.truncated?'Partial scan — more records available':'Scan complete'}</p><details><summary>Partition cursors and scan boundaries</summary><pre>{JSON.stringify({cursors:page.cursors,beginnings:page.beginnings,boundaries:page.boundaries},null,2)}</pre></details><RecordsTable records={page.records} busy={busy} onReveal={session?.roles.some(role=>role==='Admin'||role==='Operator')?record=>run(()=>revealRecord(record)):undefined}/>{!page.records.length&&<p>No matches in this scan window.</p>}<button disabled={busy||!page.truncated} onClick={()=>run(()=>browse(true))}>Next scan</button> <Help text="Continue from the previous partition cursors. A partial scan may not contain every match."/></>}
 </section></div>{session?.roles.some(r=>r==='Admin'||r==='Operator')&&selected&&topic&&<PlannerPanel key={selected} activeTab={tab} onTab={setTab} api={api} profileId={selected} topic={topic} filter={query()} roles={session.roles}/>}</main></div><footer>KAFKA DEV TOOL / READY <Help text="Browsing is read-only. Record values are masked by default. Reveal record displays readable text and labels binary data as base64. Fields marked * are required."/></footer></>;
}
createRoot(document.getElementById('root')!).render(<App/>);
