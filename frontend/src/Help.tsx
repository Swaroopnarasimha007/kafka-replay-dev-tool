import {useId,useLayoutEffect,useRef,useState} from 'react';
import {createPortal} from 'react-dom';

export function Help({text}:{text:string}) {
 const id=useId(),tip=useRef<HTMLDivElement>(null);
 const [hovered,setHovered]=useState(false),[focused,setFocused]=useState(false);
 const [anchor,setAnchor]=useState({x:0,y:0}),[position,setPosition]=useState({x:0,y:0});
 const visible=hovered||focused;
 useLayoutEffect(()=>{
  if(!visible||!tip.current)return;
  const {width,height}=tip.current.getBoundingClientRect();
  const x=Math.max(8,Math.min(anchor.x+14,window.innerWidth-width-8));
  const y=Math.max(8,anchor.y+height+18>window.innerHeight?anchor.y-height-12:anchor.y+18);
  setPosition({x,y});
 },[visible,anchor,text]);
 return <><button type="button" className="help" aria-label={text} aria-describedby={visible?id:undefined}
  onMouseEnter={e=>{setAnchor({x:e.clientX,y:e.clientY});setHovered(true)}}
  onMouseMove={e=>setAnchor({x:e.clientX,y:e.clientY})} onMouseLeave={()=>setHovered(false)}
  onFocus={e=>{if(e.currentTarget.matches(':focus-visible')){const r=e.currentTarget.getBoundingClientRect();setAnchor({x:r.right,y:r.bottom});setFocused(true)}}}
  onBlur={()=>setFocused(false)} onKeyDown={e=>{if(e.key==='Escape'){setHovered(false);setFocused(false)}}}>i</button>
  {visible&&createPortal(<div ref={tip} id={id} role="tooltip" className="help-tooltip" style={{left:position.x,top:position.y}}>{text}</div>,document.body)}</>;
}
