/* Toggleable infrastructure overlays for the constraint maps (shared by both tabs).
 * Sparse layers (wind/solar farms, data centers) load nationwide as static GeoJSON.
 * Dense layers (substations, pipelines) are zoom-gated and fetch only the current
 * viewport live from Overpass (via the Flask proxy), so the map never tries to draw
 * the whole country. All layers are OFF by default. Source: OpenStreetMap (ODbL).
 *
 *   initInfraLayers(map, chipsEl, hintEl)   // call once per map
 */
(function () {
  window.initInfraLayers = function (map, el, hintEl) {
    const LAYERS = [
      { k: "wind",        label: "Wind",      type: "Wind farm",   kind: "static", src: "/api/infra/generation",
        filter: f => f.properties.source === "wind",
        pstyle: { radius: 3, color: "#0e7490", weight: 1, fillColor: "#06b6d4", fillOpacity: 0.75 } },
      { k: "solar",       label: "Solar",     type: "Solar farm",  kind: "static", src: "/api/infra/generation",
        filter: f => f.properties.source === "solar",
        pstyle: { radius: 3, color: "#b45309", weight: 1, fillColor: "#f59e0b", fillOpacity: 0.75 } },
      { k: "datacenters", label: "Data centers", type: "Data center", kind: "static", src: "/api/infra/datacenters",
        filter: () => true,
        pstyle: { radius: 4, color: "#9d174d", weight: 1, fillColor: "#db2777", fillOpacity: 0.85 } },
      { k: "substations", label: "Substations", type: "Substation", kind: "dense", src: "/api/infra/dense/substations", minZoom: 9,
        pstyle: { radius: 3, color: "#334155", weight: 1, fillColor: "#94a3b8", fillOpacity: 0.85 } },
      { k: "pipelines",   label: "Pipelines",  type: "Pipeline",   kind: "dense", src: "/api/infra/dense/pipelines", minZoom: 9,
        lstyle: { color: "#ea580c", weight: 1.5, opacity: 0.75, dashArray: "4 3" } },
    ];
    const box = document.getElementById("mapDetails");
    const active = {}, groups = {}, staticCache = {};

    function swatch(l) { return l.pstyle ? l.pstyle.fillColor : l.lstyle.color; }       // chip dot
    function badgeColor(l) { return l.pstyle ? l.pstyle.color : l.lstyle.color; }       // darker, for white text
    function chips() {
      el.innerHTML = LAYERS.map(l =>
        `<button data-k="${l.k}" class="${active[l.k] ? "active" : ""}">` +
        `<span class="ic-dot" style="background:${swatch(l)}"></span>${l.label}</button>`).join("");
      el.querySelectorAll("button").forEach(b => b.onclick = () => toggle(b.dataset.k));
    }
    function group(k) { if (!groups[k]) groups[k] = L.layerGroup().addTo(map); return groups[k]; }
    function centroid(f) {
      const g = f.geometry;
      if (!g) return null;
      if (g.type === "Point") return g.coordinates;
      if (g.type === "LineString" && g.coordinates.length) return g.coordinates[Math.floor(g.coordinates.length / 2)];
      return null;
    }
    // Show a clicked feature in the shared bottom details box (like a constraint).
    function details(f, l) {
      if (!box) return;
      const p = f.properties || {};
      const vbadge = p.voltage ? ` <span class="md-kv">${p.voltage} kV</span>` : "";
      const rows = [];
      if (p.mw) rows.push(`Capacity&nbsp;<b>${(+p.mw).toLocaleString()} MW</b>`);
      if (p.substance) rows.push(`Carries&nbsp;<b>${p.substance}</b>`);
      if (p.source && l.kind === "static" && l.k !== "datacenters") rows.push(`Fuel&nbsp;<b>${p.source}</b>`);
      const c = centroid(f);
      if (c) rows.push(`<span style="color:#9ca3af;">${c[1].toFixed(4)}, ${c[0].toFixed(4)}</span>`);
      box.innerHTML =
        `<div class="md-t">${p.name || l.type} <span class="md-kv" style="background:${badgeColor(l)};">${l.type}</span>${vbadge}</div>` +
        `<div class="md-s">Owner&nbsp;&middot;&nbsp;${p.operator || "<i>not in OpenStreetMap</i>"}</div>` +
        (rows.length ? `<div class="md-v">${rows.join(" &nbsp;&middot;&nbsp; ")}</div>` : "");
    }
    function draw(gj, l) {
      const g = group(l.k);
      L.geoJSON(gj, {
        filter: l.filter,
        pointToLayer: (f, ll) => L.circleMarker(ll, l.pstyle),
        style: l.lstyle,
        onEachFeature: (f, ly) => ly.on("click", e => { L.DomEvent.stopPropagation(e); details(f, l); }),
      }).addTo(g);
    }
    async function loadStatic(l) {
      const g = group(l.k); g.clearLayers();
      let gj = staticCache[l.src];
      if (!gj) { gj = await (await fetch(l.src)).json(); staticCache[l.src] = gj; }
      draw(gj, l);
    }
    async function loadDense(l) {
      const g = group(l.k); g.clearLayers();
      if (map.getZoom() < l.minZoom) { showHint(); return; }
      const b = map.getBounds();
      const bbox = `${b.getSouth().toFixed(2)},${b.getWest().toFixed(2)},${b.getNorth().toFixed(2)},${b.getEast().toFixed(2)}`;
      try {
        const gj = await (await fetch(`${l.src}?bbox=${bbox}`)).json();
        if (gj && gj.features) draw(gj, l);
      } catch (e) { /* overpass hiccup -- ignore, will retry on next move */ }
    }
    function refresh(l) { if (active[l.k]) (l.kind === "static" ? loadStatic : loadDense)(l); }
    function toggle(k) {
      const l = LAYERS.find(x => x.k === k);
      active[k] = !active[k]; chips();
      if (active[k]) refresh(l);
      else if (groups[k]) { map.removeLayer(groups[k]); delete groups[k]; }
      showHint();
    }
    function showHint() {
      if (!hintEl) return;
      const need = LAYERS.some(l => l.kind === "dense" && active[l.k]) && map.getZoom() < 9;
      hintEl.textContent = need ? "zoom in to load substations / pipelines" : "";
    }
    // Re-fetch the dense (viewport) layers after pan/zoom, debounced.
    let t;
    map.on("moveend", () => {
      clearTimeout(t);
      t = setTimeout(() => { LAYERS.filter(l => l.kind === "dense" && active[l.k]).forEach(loadDense); showHint(); }, 350);
    });
    chips();
  };
})();
