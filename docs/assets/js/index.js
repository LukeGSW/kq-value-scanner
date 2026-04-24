/* ============================================================================
 * KQ Value Scanner · index.js
 * ---------------------------------------------------------------------------
 * Script di pagina per site/index.html. Si aspetta:
 *   - il JSON globale caricato da ./data/screener.json
 *   - jQuery + DataTables già disponibili a livello globale
 *   - KQ (formatters.js) già disponibile a livello globale
 * ========================================================================== */
(function(){
  'use strict';

  const JSON_URL = 'data/screener.json';

  /* Colonne della tabella — ordine == ordine visuale */
  const COLUMNS = [
    {
      key:'ticker',
      title:'Ticker',
      className:'ticker-cell text-left',
      render: r => {
        const href = `ticker.html?t=${encodeURIComponent(r.ticker)}`;
        return `<a href="${href}">${r.ticker}</a>`;
      },
    },
    {
      key:'name',
      title:'Nome',
      className:'text-left',
      render: r => r.name || KQ.NA,
    },
    {
      key:'exchange',
      title:'Exch.',
      className:'text-left',
      render: r => r.exchange || KQ.NA,
    },
    {
      key:'sector',
      title:'Settore',
      className:'text-left',
      render: r => r.sector || KQ.NA,
    },
    {
      key:'market_cap_usd',
      title:'Market Cap',
      render: r => KQ.fmtMarketCap(r.market_cap_usd),
      order: r => r.market_cap_usd ?? -1,
    },
    {
      key:'last_close',
      title:'Prezzo',
      render: r => KQ.isNum(r.last_close) ? KQ.fmtNumber(r.last_close, 2) : KQ.NA,
      order: r => r.last_close ?? -1,
    },
    {
      key:'pe_ttm',
      title:'PE TTM',
      render: r => KQ.fmtRatio(r.pe_ttm, 1),
      order: r => r.pe_ttm ?? 99999,
    },
    {
      key:'forward_pe',
      title:'Fwd PE',
      render: r => KQ.fmtRatio(r.forward_pe, 1),
      order: r => r.forward_pe ?? 99999,
    },
    {
      key:'peg',
      title:'PEG',
      render: r => {
        const f = KQ.flagPeg(r.peg);
        const cls = f==='good'?'cell-pos':(f==='bad'?'cell-neg':(f==='warn'?'cell-neutral':'cell-muted'));
        return `<span class="${cls}">${KQ.fmtNumber(r.peg, 2)}</span>`;
      },
      order: r => r.peg ?? 99999,
    },
    {
      key:'pe_percentile_5y',
      title:'PE %ile 5Y',
      render: r => {
        if(!KQ.isNum(r.pe_percentile_5y)) return KQ.NA;
        const f = KQ.flagPePercentile(r.pe_percentile_5y);
        const cls = f==='good'?'cell-pos':(f==='bad'?'cell-neg':'cell-neutral');
        return `<span class="${cls}">${KQ.fmtPct(r.pe_percentile_5y, 0)}</span>`;
      },
      order: r => r.pe_percentile_5y ?? -1,
    },
    {
      key:'ev_to_ebitda',
      title:'EV/EBITDA',
      render: r => KQ.fmtRatio(r.ev_to_ebitda, 1),
      order: r => r.ev_to_ebitda ?? 99999,
    },
    {
      key:'fcf_yield',
      title:'FCF Yield',
      render: r => {
        if(!KQ.isNum(r.fcf_yield)) return KQ.NA;
        const cls = r.fcf_yield > 0.05 ? 'cell-pos' :
                    (r.fcf_yield > 0 ? 'cell-neutral' : 'cell-neg');
        return `<span class="${cls}">${KQ.fmtPct(r.fcf_yield, 2)}</span>`;
      },
      order: r => r.fcf_yield ?? -99,
    },
    {
      key:'dividend_yield',
      title:'Div Yield',
      render: r => KQ.fmtPct(r.dividend_yield, 2),
      order: r => r.dividend_yield ?? -1,
    },
    {
      key:'net_debt_ebitda',
      title:'ND/EBITDA',
      render: r => {
        if(!KQ.isNum(r.net_debt_ebitda)) return KQ.NA;
        const f = KQ.flagNetDebtEbitda(r.net_debt_ebitda);
        const cls = f==='good'?'cell-pos':(f==='warn'?'cell-neutral':'cell-neg');
        return `<span class="${cls}">${KQ.fmtNumber(r.net_debt_ebitda, 2)}x</span>`;
      },
      order: r => r.net_debt_ebitda ?? 99999,
    },
    {
      key:'roic_ttm',
      title:'ROIC',
      render: r => {
        if(!KQ.isNum(r.roic_ttm)) return KQ.NA;
        const f = KQ.flagRoic(r.roic_ttm);
        const cls = f==='good'?'cell-pos':(f==='warn'?'cell-neutral':'cell-neg');
        return `<span class="${cls}">${KQ.fmtPct(r.roic_ttm, 1)}</span>`;
      },
      order: r => r.roic_ttm ?? -99,
    },
    {
      key:'altman_z',
      title:'Altman Z',
      render: r => KQ.fmtNumber(r.altman_z, 2),
      order: r => r.altman_z ?? -99,
    },
    {
      key:'piotroski_f',
      title:'Piotroski',
      render: r => {
        if(!KQ.isNum(r.piotroski_f)) return KQ.NA;
        const f = KQ.flagPiotroski(r.piotroski_f);
        const cls = f==='good'?'cell-pos':(f==='warn'?'cell-neutral':'cell-neg');
        return `<span class="${cls}">${r.piotroski_f}/9</span>`;
      },
      order: r => r.piotroski_f ?? -1,
    },
    {
      key:'one_year_return',
      title:'1Y ret.',
      render: r => {
        if(!KQ.isNum(r.one_year_return)) return KQ.NA;
        return `<span class="${KQ.signClass(r.one_year_return)}">${KQ.fmtPctSigned(r.one_year_return, 1)}</span>`;
      },
      order: r => r.one_year_return ?? -99,
    },
    {
      key:'kq_value_score',
      title:'KQ Score',
      render: r => {
        if(!KQ.isNum(r.kq_value_score)) return KQ.NA;
        const cls = KQ.flagKqScore(r.kq_value_score);
        return `<span class="score-pill s-${cls}">${KQ.fmtNumber(r.kq_value_score, 1)}</span>`;
      },
      order: r => r.kq_value_score ?? -1,
    },
    {
      key:'rank_global',
      title:'Rank',
      render: r => KQ.isNum(r.rank_global) ? '#' + r.rank_global : KQ.NA,
      order: r => r.rank_global ?? 99999,
    },
  ];

  function headerRow(){
    return '<tr>' + COLUMNS.map(c => `<th class="${c.className||''}">${c.title}</th>`).join('') + '</tr>';
  }

  function bodyRow(row){
    return '<tr>' + COLUMNS.map(c => {
      const html = c.render(row);
      return `<td class="${c.className||''}">${html}</td>`;
    }).join('') + '</tr>';
  }

  function populateSelect(sel, values){
    sel.innerHTML = '<option value="">Tutti</option>';
    for(const v of values){
      if(!v) continue;
      const opt = document.createElement('option');
      opt.value = v; opt.textContent = v;
      sel.appendChild(opt);
    }
  }

  function renderStats(meta){
    const $s = document.getElementById('summaryStats');
    $s.innerHTML = `
      <div class="stat-card">
        <div class="stat-label">Ticker in universe</div>
        <div class="stat-value mono">${KQ.fmtInt(meta.ticker_count)}</div>
        <div class="stat-sub">Large + Mid Cap USA & Europa</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Exchange coperti</div>
        <div class="stat-value mono">${(meta.exchanges||[]).length}</div>
        <div class="stat-sub">${(meta.exchanges||[]).join(' · ')}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Settori distinti</div>
        <div class="stat-value mono">${(meta.sectors||[]).length}</div>
        <div class="stat-sub">Classificazione GICS</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Ultimo aggiornamento</div>
        <div class="stat-value mono" style="font-size:15px">${KQ.fmtDate(meta.build_ts_utc)}</div>
        <div class="stat-sub">Schema v${meta.schema_version}</div>
      </div>
    `;
  }

  async function boot(){
    const $table = document.getElementById('screenerTable');
    const $loading = document.getElementById('loadingBox');
    const $error = document.getElementById('errorBox');

    try{
      /* Cache-busting manuale: alcuni CDN (incluso GitHub Pages) servono
         varianti stale nonostante `cache:'no-store'`. Aggiungiamo un
         timestamp garantito unique-per-fetch. */
      const bust = 'v=' + Date.now();
      const url = JSON_URL + '?' + bust;
      const payload = await KQ.loadJson(url);
      const meta = payload.meta || {};
      const rows = payload.rows || [];

      /* Log diagnostico visibile in DevTools → Console */
      console.log('[KQ] screener.json fetched:', {
        url,
        rows_count: rows.length,
        ticker_count_meta: meta.ticker_count,
        build_ts_utc: meta.build_ts_utc,
        schema_version: meta.schema_version,
        first_row: rows[0] ? rows[0].ticker : null,
        sample_fields_first_row: rows[0] ? {
          pe_ttm: rows[0].pe_ttm,
          forward_pe: rows[0].forward_pe,
          market_cap_usd: rows[0].market_cap_usd,
          last_close: rows[0].last_close,
        } : null,
      });

      renderStats(meta);

      /* Popola filtri */
      const exchSel = document.getElementById('exchangeFilter');
      const secSel = document.getElementById('sectorFilter');
      populateSelect(exchSel, meta.exchanges || []);
      populateSelect(secSel, meta.sectors || []);

      /* Costruisci HTML tabella */
      $table.querySelector('thead').innerHTML = headerRow();
      $table.querySelector('tbody').innerHTML = rows.map(bodyRow).join('');

      /* Rimuovi overlay */
      $loading.style.display = 'none';

      /* Inizializza DataTables — i valori di ordering sono presi dai data-order.
         `scrollX:true` → DataTables wrappa la tabella in un contenitore con
         overflow-x:auto, mantiene header e body sincronizzati in larghezza e
         permette di scorrere orizzontalmente su tutte le 20 colonne. */
      const dt = window.jQuery('#screenerTable').DataTable({
        pageLength: 50,
        lengthMenu: [25, 50, 100, 250, 500],
        order: [[COLUMNS.findIndex(c => c.key==='kq_value_score'), 'desc']],
        autoWidth: false,
        scrollX: true,
        deferRender: true,
        language: {
          sProcessing:    'Elaborazione…',
          sLengthMenu:    'Mostra _MENU_ righe',
          sZeroRecords:   'Nessun risultato',
          sInfo:          'Riga _START_ - _END_ di _TOTAL_',
          sInfoEmpty:     '0 risultati',
          sInfoFiltered:  '(filtrata da _MAX_ totali)',
          sSearch:        'Cerca:',
          sEmptyTable:    'Tabella vuota',
          oPaginate: {
            sFirst:    '«',
            sLast:     '»',
            sNext:     '›',
            sPrevious: '‹',
          },
        },
        columnDefs: COLUMNS.map((c, idx) => ({
          targets: idx,
          orderData: c.order ? undefined : idx,
        })),
      });

      /* Filtri custom */
      function applyFilters(){
        const ex = exchSel.value;
        const se = secSel.value;
        dt.rows().every(function(){
          const row = this.data();
          const html = row.join('|');  /* hack lite: non abbiamo oggetti a runtime */
          return true;
        });
        /* Per semplicità: filtro via search globale custom */
        window.jQuery.fn.dataTable.ext.search = [];
        window.jQuery.fn.dataTable.ext.search.push(function(settings, data){
          /* data è l'array di cell html — indici 2 (exchange) e 3 (sector) sono text */
          const htmlExch = data[2] || '';
          const htmlSec = data[3] || '';
          if(ex && !htmlExch.includes(ex)) return false;
          if(se && !htmlSec.includes(se)) return false;
          return true;
        });
        dt.draw();
      }
      exchSel.addEventListener('change', applyFilters);
      secSel.addEventListener('change', applyFilters);

    }catch(err){
      console.error(err);
      $loading.style.display = 'none';
      $error.style.display = 'block';
      $error.textContent = 'Errore caricamento screener.json · ' + (err.message || err);
    }
  }

  document.addEventListener('DOMContentLoaded', boot);

  /* Se torniamo dalla pagina ticker via back/forward e il browser ha usato
     il bfcache (pageshow.persisted === true), il DOM è "congelato" e
     DataTables può trovarsi in stato inconsistente. Forziamo un reload
     completo per garantire una pagina sempre fresca. */
  window.addEventListener('pageshow', function(e){
    if(e.persisted){
      console.log('[KQ] bfcache hit → reload forzato');
      window.location.reload();
    }
  });
})();
