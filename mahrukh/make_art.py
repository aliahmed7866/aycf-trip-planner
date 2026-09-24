"""Rebuild original SVG illustrations. Standard library only; no image downloads."""
from pathlib import Path
from html import escape

OUT = Path(__file__).parent / 'static' / 'art'
PALETTES = [
 ('emerald-pret','Emerald embroidered shalwar kameez','pret','#164e40','#092c28','#d4af64','#182a24'),
 ('rose-unstitched','Rose embroidered unstitched fabric','fabric','#ac6975','#613747','#ecc4a1','#33262b'),
 ('midnight-abaya','Midnight abaya with gold embroidery','abaya','#242c38','#101722','#bdad82','#202632'),
 ('ruby-lehenga','Ruby festive lehenga and dupatta','lehenga','#963347','#541a31','#e4bd6b','#35232a'),
 ('ivory-formal','Ivory embroidered long formal dress','formal','#e0d2b5','#a79b7d','#765c37','#34352d'),
 ('indigo-pret','Indigo patterned shalwar kameez','pret','#3f528e','#1d2c55','#c6bd9a','#222939'),
 ('saffron-festive','Saffron festive peshwas dress','lehenga','#c18a32','#865023','#edce90','#342c20'),
 ('plum-formal','Plum embroidered formal dress','formal','#714662','#3d273f','#d5b888','#2d2530'),
]

def render(slug, title, kind, light, dark, gold, bg):
    defs=f'''<defs><linearGradient id="cloth" x1="0" x2="1"><stop stop-color="{dark}"/><stop offset=".38" stop-color="{light}"/><stop offset=".7" stop-color="{light}"/><stop offset="1" stop-color="{dark}"/></linearGradient><linearGradient id="scarf" x1="0" x2="1"><stop stop-color="{light}" stop-opacity=".8"/><stop offset="1" stop-color="{dark}" stop-opacity=".95"/></linearGradient><radialGradient id="glow"><stop stop-color="{light}" stop-opacity=".18"/><stop offset="1" stop-color="{bg}" stop-opacity="0"/></radialGradient><pattern id="weave" width="6" height="6" patternUnits="userSpaceOnUse"><path d="M0 0h6M0 3h6" stroke="{gold}" opacity=".06" stroke-width=".4"/></pattern><g id="flower" stroke="{gold}" fill="none" stroke-width="1"><path d="M0-7C-9-16-13-5-5 0C-16 3-8 14 0 6C7 15 15 4 6 0C15-7 6-16 0-7Z"/><circle r="2" fill="{gold}"/><path d="M0 7v15m0-7q-10-7-9 0q5 6 9 5m0-1q10-9 10-1q-5 6-10 6"/></g><pattern id="motif" width="47" height="56" patternUnits="userSpaceOnUse"><use href="#flower" transform="translate(23 22) scale(.48)" opacity=".65"/></pattern></defs>'''
    background=f'''<rect width="480" height="600" fill="{bg}"/><rect width="480" height="600" fill="url(#glow)"/><path d="M48 553V230a192 192 0 0 1 384 0v323Z" fill="none" stroke="{gold}" opacity=".23"/><path d="M61 553V232a179 179 0 0 1 358 0v321" fill="none" stroke="{gold}" opacity=".11"/><path d="M34 557h412M34 565h412" stroke="{gold}" opacity=".18"/><ellipse cx="245" cy="535" rx="143" ry="18" fill="#000" opacity=".2"/>'''
    # Each silhouette is separately drawn; ornament shares a coherent botanical vocabulary.
    if kind=='fabric':
        shape=f'''<g transform="rotate(-12 240 330)"><path d="M90 225h285v212H90Z" fill="url(#cloth)"/><path d="M90 225h285v212H90Z" fill="url(#motif)"/><path d="M90 245h285M90 258h285M90 409h285M90 421h285" stroke="{gold}" stroke-width="3"/><path d="M111 230v203M354 230v203" stroke="{gold}" opacity=".5"/></g><g transform="rotate(8 245 415)"><rect x="114" y="355" width="280" height="137" rx="3" fill="url(#cloth)"/><rect x="114" y="355" width="280" height="137" fill="url(#motif)"/><path d="M126 360v128m13-128v128m232-128v128m12-128v128" stroke="{gold}" stroke-width="2"/><path d="M148 369h208M148 477h208" stroke="{gold}" opacity=".5"/></g><path d="M108 186Q236 143 334 170L292 335Q214 292 86 330Z" fill="url(#scarf)" stroke="{gold}"/><path d="M112 196Q235 156 324 181L286 323Q213 288 96 317Z" fill="url(#motif)" stroke="{gold}" opacity=".7"/>'''
    else:
        scarf=f'''<path d="M305 158Q364 151 383 219L411 494Q382 521 352 493L314 239Z" fill="url(#scarf)" stroke="{gold}" stroke-width="1.5"/><path d="M319 168Q355 165 369 220L398 489M355 184L380 495" fill="none" stroke="{gold}" opacity=".65"/><path d="M352 475l55 7m-53 6l54 7" stroke="{gold}" stroke-width="2"/>'''
        if kind=='pret':
            silhouette='M187 169L147 192L114 292L149 308L175 242L164 426Q240 444 315 426L301 242L328 308L363 292L332 192L286 169Q241 196 187 169Z'
            shape=f'''<path d="M181 410L174 521L219 526L238 442L254 526L300 521L294 410Z" fill="url(#cloth)" stroke="{gold}" stroke-opacity=".3"/><path d="M174 510l45 5m36 0l44-5" stroke="{gold}" stroke-width="3"/>{scarf}<path d="{silhouette}" fill="url(#cloth)"/><path d="{silhouette}" fill="url(#motif)"/><path d="M168 408Q240 426 311 408M167 418Q240 436 313 418M121 279l32 14m170 1l32-14" fill="none" stroke="{gold}" stroke-width="3"/><path d="M188 171Q240 209 286 171L270 188L244 257L216 191Z" fill="{dark}" stroke="{gold}"/><path d="M238 197v48m-8-45v29m17-28v28" stroke="{gold}"/><path d="M186 264l-6 131m109-131l9 131" stroke="{gold}" opacity=".2"/>'''
        elif kind=='abaya':
            silhouette='M205 135Q239 110 273 135L307 170L371 340L332 363L291 252L343 524Q240 556 133 524L185 252L144 363L106 340L172 170Z'
            shape=f'''<path d="{silhouette}" fill="url(#cloth)" stroke="{gold}" stroke-opacity=".25"/><path d="M202 139Q239 172 277 139L261 190L241 523L220 190Z" fill="{dark}"/><path d="M201 144L225 194L233 525M277 144L254 194L246 525" fill="none" stroke="{gold}" stroke-width="2"/><path d="M172 173L122 339m185-166l49 166" stroke="{gold}" stroke-width="2"/><path d="M143 512Q240 540 334 512" fill="none" stroke="{gold}" stroke-width="2"/><path d="M197 265l-37 236m121-236l36 236" stroke="{light}" stroke-width="3" opacity=".7"/>'''
            shape+=''.join(f'<use href="#flower" transform="translate({x} {y}) scale(.6)"/>' for x,y in [(183,213),(295,213),(172,240),(306,240),(161,267),(317,267),(224,360),(252,392),(224,440),(252,472)])
        else:
            silhouette='M196 161L162 178L119 283L149 298L187 233L202 281L128 513Q240 553 349 513L277 281L292 233L329 298L359 283L317 178L280 161Q238 187 196 161Z'
            if kind=='formal':
                silhouette='M195 160L161 179L126 309L156 316L188 235L191 289L157 522Q240 548 322 522L285 289L288 235L322 316L352 309L318 179L282 160Q240 189 195 160Z'
            shape=f'''{scarf}<path d="{silhouette}" fill="url(#cloth)"/><path d="{silhouette}" fill="url(#motif)"/><path d="M197 166Q240 207 279 166L261 224Q240 247 215 224Z" fill="{dark}" stroke="{gold}" stroke-width="2"/><path d="M202 271Q240 285 277 271M201 280Q240 294 279 280" stroke="{gold}" fill="none" stroke-width="3"/>'''
            for x in range(172,321,25):
                shape+=f'<path d="M240 291Q{x} 400 {x} 512" stroke="{gold}" fill="none" opacity=".2"/>'
            shape+=f'<path d="M155 505Q240 537 323 505M156 516Q240 544 322 516" fill="none" stroke="{gold}" stroke-width="3"/>'
            shape+=''.join(f'<use href="#flower" transform="translate({x} {497 + (10 if 200<x<290 else 0)}) scale(.7)"/>' for x in range(170,320,22))
        shape+=f'<path d="M219 133q-4-20 17-22q18-1 15 17l-9 12v10m-48 19l48-19l45 19" fill="none" stroke="{gold}" stroke-width="2" opacity=".65"/>'
    return f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 480 600" role="img" aria-labelledby="title"><title id="title">{escape(title)} — original Mahrukh digital illustration</title>{defs}{background}{shape}<rect width="480" height="600" fill="url(#weave)" pointer-events="none"/><path d="M225 575h30" stroke="{gold}" opacity=".6"/><circle cx="240" cy="575" r="3" fill="{gold}"/></svg>'

if __name__=='__main__':
    OUT.mkdir(parents=True,exist_ok=True)
    for palette in PALETTES:
        (OUT / (palette[0]+'.svg')).write_text(render(*palette))
    print('Created eight original clothing SVGs.')
