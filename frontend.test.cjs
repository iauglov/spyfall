const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync('static/index.html', 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const players = [
  {id:'a', name:'Аня', alive:true, connected:true},
  {id:'b', name:'Борис', alive:true, connected:true},
  {id:'c', name:'Вера', alive:true, connected:true},
];

function client(id) {
  const elements = new Map();
  function element(tag='div') {
    const classes = new Set();
    return {
      tag, style:{}, children:[], open:false, disabled:false, textContent:'',
      classList:{
        add(v){classes.add(v)}, remove(v){classes.delete(v)},
        contains(v){return classes.has(v)},
        toggle(v,on){if(on) classes.add(v); else classes.delete(v)},
      },
      replaceChildren(){this.children=[]},
      appendChild(child){this.children.push(child)},
      querySelectorAll(tag){return this.children.filter(child=>child.tag===tag)},
      showModal(){this.open=true}, close(){this.open=false},
    };
  }
  for(const match of html.matchAll(/<([a-z0-9]+)[^>]*\bid="([^"]+)"/g)) {
    elements.set(match[2],element(match[1]));
  }
  const sent = [];
  const context = vm.createContext({
    document:{
      getElementById(id){assert.ok(elements.has(id),'Unknown HTML id: '+id); return elements.get(id)},
      createElement:element,
      querySelectorAll(selector){
        assert.equal(selector,'.screen');
        return [...elements].filter(([id])=>id.startsWith('screen-')).map(([,e])=>e);
      },
    },
    window:{addEventListener(){}},
    localStorage:{setItem(){},removeItem(){}},
    WebSocket:{OPEN:1},
    setInterval(){return 1}, clearInterval(){}, setTimeout(){}, clearTimeout(){},
    confirm:()=>true,
    emit(value){sent.push(JSON.parse(value))},
  });
  vm.runInContext(script,context);
  context.me=id;
  context.fixture={role:'spy',locations:['Банк','Школа'],players};
  vm.runInContext("myID=me; ws={readyState:1,send(v){emit(v)}}; onGameStarted(fixture);",context);
  return {
    node:id=>elements.get(id),
    sent,
    run:code=>vm.runInContext(code,context),
    flow(payload){context.payload=JSON.parse(JSON.stringify(payload)); vm.runInContext('onRoundState(payload)',context)},
    reconnect(payload){context.payload=payload; vm.runInContext('onReconnected(payload)',context)},
  };
}

const turn=(revision,asker='a',answerer='b',confirmed={})=>({
  revision,state:'playing',round:1,players,
  turn:{id:revision===1?10:11,asker_id:asker,answerer_id:answerer,confirmed},vote:null,
});

test('only the current pair sees the question; both acknowledgements transfer the question',()=>{
  const a=client('a'), b=client('b'), c=client('c');
  const start=turn(1);
  for(const browser of [a,b,c]) browser.flow(start);
  assert.equal(a.node('turn-dialog').open,true);
  assert.equal(b.node('turn-dialog').open,true);
  assert.equal(c.node('turn-dialog').open,false);
  assert.match(a.node('turn-description').textContent,/Борис/);
  assert.match(b.node('turn-description').textContent,/Аня/);
  a.run('confirmTurn()');
  assert.deepEqual(a.sent,[{type:'turn_confirm',payload:{turn_id:10}}]);
  const acknowledged={...start,revision:2,turn:{...start.turn,confirmed:{a:true}}};
  a.flow(acknowledged); b.flow(acknowledged);
  assert.equal(a.node('turn-dialog').open,false);
  assert.equal(b.node('turn-dialog').open,true);
  const next=turn(3,'b','c');
  for(const browser of [a,b,c]) browser.flow(next);
  assert.equal(a.node('turn-dialog').open,false);
  assert.equal(b.node('turn-dialog').open,true);
  assert.equal(c.node('turn-dialog').open,true);
  b.flow(start);
  assert.match(b.node('turn-description').textContent,/Вера/,'stale packet must not rewind turn');
});

test('round vote opens for everyone; acknowledged vote and reconnect preserve the waiting state',()=>{
  const a=client('a');
  const voting={revision:5,state:'voting',round:1,players,turn:null,vote:{id:20,voted:[],vote_seconds:30}};
  a.flow(voting);
  assert.equal(a.node('round-vote-dialog').open,true);
  assert.equal(a.node('turn-dialog').open,false);
  const options=a.node('round-vote-options').children;
  assert.deepEqual(options.map(b=>b.textContent),['Аня (вы)','Борис','Вера','Пропустить']);
  options[1].onclick();
  assert.deepEqual(a.sent,[{type:'round_vote',payload:{vote_id:20,target_id:'b'}}]);
  const accepted={...voting,revision:6,vote:{...voting.vote,voted:['a']}};
  a.flow(accepted);
  assert.equal(a.node('round-vote-options').children.length,0);
  a.reconnect({state:'voting',your_id:'a',code:'ABCD',host_id:'a',players,role:'spy',alive:true,round_state:accepted});
  assert.equal(a.node('round-vote-dialog').open,true);
  assert.equal(a.node('round-vote-options').children.length,0);
  a.flow({...turn(7),round:2});
  assert.equal(a.node('round-vote-dialog').open,false);
  assert.equal(a.node('turn-dialog').open,true);
});

test('reconnect restores the unconfirmed respondent and a finished game closes all dialogs',()=>{
  const b=client('b');
  const flow={...turn(1),turn:{...turn(1).turn,confirmed:{a:true}}};
  b.reconnect({state:'playing',your_id:'b',code:'ABCD',host_id:'a',players,role:'civilian',alive:true,round_state:flow});
  assert.equal(b.node('turn-dialog').open,true);
  b.run("onGameOver({winner:'civilians',spies:[],location:'Банк',reason:'all_spies_caught'})");
  assert.equal(b.node('turn-dialog').open,false);
  assert.equal(b.node('round-vote-dialog').open,false);
  b.flow(turn(9));
  assert.equal(b.node('turn-dialog').open,false);
});

test('an accusation without elimination restores the pending question modal',()=>{
  const a=client('a');
  const current=turn(1);
  a.flow(current);
  assert.equal(a.node("turn-dialog").open,true);
  a.run("onAccuseStarted({accuser_id:'a',accuser_name:'Аня',target_id:'c',target_name:'Вера',vote_seconds:30})");
  assert.equal(a.node("turn-dialog").open,false);
  a.run("onAccuseResult({target_id:'c',target_name:'Вера',target_died:false,majority:false,guilty_votes:0,innocent_votes:1})");
  a.flow({...current,revision:2});
  assert.equal(a.node("turn-dialog").open,true);
});
