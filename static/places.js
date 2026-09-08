(() => {
  'use strict';
  const root = document.querySelector('.journal');
  const api = root.dataset.api;
  const countries = JSON.parse(document.getElementById('countries-data').textContent);
  const catalog = new Map(countries.map(c => [c.code, c]));
  const $ = id => document.getElementById(id);
  const dialog = $('place-dialog'), form = $('place-form');
  let records = [], filter = 'all', editing = null, zoom = 1;
  const field = name => form.elements.namedItem(name);
  function node(tag, text, cls) { const el = document.createElement(tag); if(text) el.textContent=text; if(cls) el.className=cls; return el; }
  async function request(url, options = {}) {
    const response = await fetch(url, {...options, headers: {'Content-Type':'application/json', 'X-CSRF-Token':root.dataset.csrf}, cache:'no-store'});
    if (!response.ok) { let body; try { body = await response.json(); } catch {} throw new Error(body?.error || 'Could not save your change. Please try again.'); }
    return response.status === 204 ? null : response.json();
  }
  function showEditor(record = null, country = '') {
    editing = record?.id ?? null; form.reset();
    for (const key of ['country','place','status','visited_on','notes']) field(key).value = record?.[key] ?? (key === 'status' ? 'visited' : key === 'country' ? country : '');
    $('dialog-title').textContent = editing ? 'A place in your story' : 'Add a place';
    $('delete-place').hidden = !editing;
    $('form-error').textContent = '';
    field('visited_on').max = new Date().toLocaleDateString('en-CA');
    $('date-label').hidden = field('status').value === 'wishlist';
    dialog.showModal();
  }
  function render() {
    const visited = records.filter(r=>r.status==='visited');
    const visitedCountries = new Set(visited.map(r=>r.country));
    const wantedCountries = new Set(records.filter(r=>r.status==='wishlist').map(r=>r.country));
    $('countries-total').textContent=visitedCountries.size;
    $('places-total').textContent=visited.length;
    $('continents-total').textContent=new Set(visited.map(r=>catalog.get(r.country)?.continent).filter(Boolean)).size;
    $('wishlist-total').textContent=records.filter(r=>r.status==='wishlist').length;
    document.querySelectorAll('.world-svg path').forEach(path=>{
      path.classList.toggle('visited',visitedCountries.has(path.dataset.code));
      path.classList.toggle('wishlist',!visitedCountries.has(path.dataset.code)&&wantedCountries.has(path.dataset.code));
    });
    const query=$('place-search').value.trim().toLocaleLowerCase();
    const shown=records.filter(r=>(filter==='all'||r.status===filter)&&`${catalog.get(r.country)?.name} ${r.place} ${r.notes}`.toLocaleLowerCase().includes(query));
    const cards=$('place-cards'); cards.replaceChildren();
    if (!shown.length) {
      const empty=node('div','', 'journal-empty');
      empty.append(node('h3',records.length ? 'No places match just yet.' : 'Every journey starts somewhere.'));
      empty.append(node('p',records.length ? 'Try another search or filter.' : 'Add your first country, favourite city or unforgettable trail.'));
      if(!records.length){const add=node('button','＋ Add your first place','earth-button');add.onclick=()=>showEditor();empty.append(add);}
      cards.append(empty);
    }
    shown.forEach(record=>{
      const country=catalog.get(record.country)?.name || record.country;
      const card=node('button','',`place-card ${record.status}`); card.type='button';
      card.append(node('span',country,'country-label'),node('h3',record.place||country));
      if(record.notes) card.append(node('p',record.notes.length>170 ? record.notes.slice(0,170)+'…' : record.notes));
      const footer=node('footer'); footer.append(node('span',record.status==='visited'?'● Visited':'● Wishlist','badge-status'),node('span',record.visited_on ? new Date(record.visited_on+'T12:00:00').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}) : 'Edit entry ↗'));
      card.append(footer);card.onclick=()=>showEditor(record);cards.append(card);
    });
  }
  async function load() { const data=await request(api); records=data.places;render(); }
  $('add-place').onclick=()=>showEditor();
  $('close-dialog').onclick=()=>dialog.close();
  field('status').onchange=()=>{$('date-label').hidden=field('status').value==='wishlist';};
  $('place-search').oninput=render;
  document.querySelectorAll('[data-filter]').forEach(button=>button.onclick=()=>{
    filter=button.dataset.filter;
    document.querySelectorAll('[data-filter]').forEach(b=>{b.classList.toggle('selected',b===button);b.setAttribute('aria-pressed',String(b===button));});render();
  });
  form.onsubmit=async event=>{
    event.preventDefault();$('save-place').disabled=true;$('form-error').textContent='';
    try {
      const data=Object.fromEntries(new FormData(form));
      const result=await request(editing ? `${api}/${editing}` : api,{method:editing?'PUT':'POST',body:JSON.stringify(data)});
      // Update immediately after confirmed persistence; a failed refresh cannot duplicate a save.
      const record={...data,id:result.id}; if(record.status==='wishlist') record.visited_on='';
      records=records.filter(r=>r.id!==record.id);records.unshift(record);render();dialog.close();$('journal-message').textContent='Place saved.';
    } catch(error){$('form-error').textContent=error.message;} finally {$('save-place').disabled=false;}
  };
  $('delete-place').onclick=async()=>{
    if(!confirm('Delete this place and its memory from your journal?'))return;
    $('delete-place').disabled=true;
    try {await request(`${api}/${editing}`,{method:'DELETE'});records=records.filter(r=>r.id!==editing);render();dialog.close();$('journal-message').textContent='Place deleted.';}
    catch(error){$('form-error').textContent=error.message;}finally{$('delete-place').disabled=false;}
  };
  function setZoom(value){zoom=Math.max(1,Math.min(4,value));const svg=document.querySelector('.world-svg');if(svg)svg.style.width=`${zoom*100}%`;}
  $('zoom-in').onclick=()=>setZoom(zoom+.5);$('zoom-out').onclick=()=>setZoom(zoom-.5);$('zoom-reset').onclick=()=>setZoom(1);
  fetch('/static/places-world.svg').then(r=>{if(!r.ok)throw new Error();return r.text();}).then(svg=>{
    // Trusted, bundled Natural Earth asset; never insert journal text as HTML.
    $('world-map').innerHTML=svg;
    document.querySelectorAll('.world-svg path').forEach(path=>{
      const name=catalog.get(path.dataset.code)?.name || path.getAttribute('aria-label');
      path.addEventListener('click',()=>showEditor(null,path.dataset.code));
      path.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();showEditor(null,path.dataset.code);}});
      path.addEventListener('mouseenter',()=>{$('map-caption').textContent=name;});
      path.addEventListener('focus',()=>{$('map-caption').textContent=name;});
    });render();
  }).catch(()=>{$('map-caption').textContent='Map unavailable. You can still add places with the button above.';});
  load().catch(()=>{$('journal-message').textContent='Your journal could not be loaded. Refresh the page to try again.';});
})();
