export function Help({text}:{text:string}) {
 return <button type="button" className="help" aria-label={text} data-tooltip={text}>i</button>;
}
