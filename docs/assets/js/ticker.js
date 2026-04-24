/* ============================================================================
 * KQ Value Scanner · ticker.js
 * ---------------------------------------------------------------------------
 * Script di pagina per site/ticker.html?t=TICKER.EXCH
 * Carica data/tickers/{ticker}.json e popola tutti i blocchi della pagina di
 * dettaglio (hero, snapshot, cash/debt, qualità, profittabilità, grafico 10Y,
 * diagnostica, stagionalità, scores, AI read).
 *
 * Richiede:
 *   - window.LightweightCharts (TradingView)
 *   - window.Chart (Chart.js)
 *   - window.KQ (formatters.js)
 * ========================================================================== */
(function(){
  'use strict';

  const K = window.KQ;
  const LW = window.LightweightCharts;

  /* ---- query string --------------------------------------------------- */
  function getTickerParam(){
    const params = new URLSearchParams(window.location.search);
    return (params.get('t') || '').trim();
  }

  /* ---- SVG ring (score) ----------------------------------------------- */
  function ringSvg(fraction, colorVar){
    const r = 50, c = 2 * Math.PI * r; // 314
    const frac = Math.max(0, Math.min(1, fraction));
    const offset = c * (1 - frac);
    const col = colorVar || 'var(--positive)';
    return `<svg width="120" height="120" viewBox="0 0 120 120">
      <circle cx="60" cy="60" r="${r}" stroke="var(--bg-card-elev)" stroke-width="10" fill="none"/>
      <circle cx="60" cy="60" r="${r}" stroke="${col}" stroke-width="10" fill="none"
              stroke-dasharray="${c.toFixed(1)}" stroke-dashoffset="${offset.toFixed(1)}"
              stroke-linecap="round"/>
    </svg>`;
  }

  function scoreColorVar(rating){
    if(['good','strong','clean','safe'].includes(rating)) return 'var(--positive)';
    if(['warn','neutral','grey','alert','mid'].includes(rating)) return 'var(--neutral)';
    if(['bad','distress','weak'].includes(rating)) return 'var(--negative)';
    return 'var(--text-muted)';
  }

  /* ---- HERO ----------------------------------------------------------- */
  function renderHero(payload){
    const m = payload.meta || {};
    const h = payload.hero || {};

    document.title = `${m.ticker} · Kriterion Quant Screener`;

    document.getElementById('tickerBadge').textContent = m.code || m.ticker || '—';
    document.getElementById('companyName').textContent = m.name || '';

    const metaWrap = document.getElementById('heroMeta');
    const chips = [];
    if(m.exchange_name) chips.push(`<span class="meta-chip">${m.exchange_name}</span>`);
    if(m.country) chips.push(`<span class="meta-chip">${m.country}</span>`);
    if(m.sector){
      const ind = m.industry ? ` · ${m.industry}` : '';
      chips.push(`<span class="meta-chip">${m.sector}${ind}</span>`);
    }
    if(h.next_earnings_date){
      chips.push(`<span class="meta-chip earnings">⊙ Next earnings: ${K.fmtDate(h.next_earnings_date)}</span>`);
    }
    metaWrap.innerHTML = chips.join('');

    /* prezzo e delta giornaliero — stimato dalla variazione 1Y sui tier */
    const price = h.last_close;
    document.getElementById('heroPrice').textContent =
      K.isNum(price) ? '$' + K.fmtNumber(price, 2) : K.NA;

    const oneY = h.one_year_return;
    const deltaEl = document.getElementById('heroDelta');
    if(K.isNum(oneY)){
      const cls = oneY >= 0 ? 'pos' : 'neg';
      const glyph = oneY >= 0 ? '▲' : '▼';
      deltaEl.className = 'price-delta ' + cls;
      deltaEl.innerHTML = `${glyph} 1Y ${K.fmtPctSigned(oneY, 2)}`;
    }else{
      deltaEl.className = 'price-delta';
      deltaEl.textContent = K.NA;
    }

    const subParts = [];
    if(K.isNum(h.market_cap_usd)) subParts.push(`Market Cap <b>${K.fmtMarketCap(h.market_cap_usd)}</b>`);
    if(K.isNum(h.beta)) subParts.push(`Beta <b>${K.fmtNumber(h.beta, 2)}</b>`);
    if(h.last_close_date) subParts.push(`Close <b>${K.fmtDate(h.last_close_date)}</b>`);
    document.getElementById('heroSub').innerHTML = subParts.join(' · ');
  }

  /* ---- SNAPSHOT 7 card ------------------------------------------------ */
  function renderSnapshot(payload){
    const s = payload.snapshot || {};
    const container = document.getElementById('snapshotGrid');

    const cards = [
      {label:'PE TTM', value:K.fmtRatio(s.pe_ttm,1), sub:'', barW:_scaleOther(s.pe_ttm, 0, 40), col:_colForPe(s.pe_ttm)},
      {label:'Forward PE', value:K.fmtRatio(s.forward_pe,1), sub:K.isNum(s.earnings_growth_next_year)?`EPS FY+1 ${K.fmtPctSigned(s.earnings_growth_next_year,1)}`:'', barW:_scaleOther(s.forward_pe,0,40), col:_colForPe(s.forward_pe)},
      {label:'PEG', value:K.fmtNumber(s.peg,2), sub:'&lt;1 sconto · &gt;2 premio', barW:_scaleOther(s.peg,0,3), col:_colForPeg(s.peg)},
      {label:'EV / EBITDA', value:K.fmtRatio(s.ev_to_ebitda,1), sub:'', barW:_scaleOther(s.ev_to_ebitda,0,30), col:_colForEvEbitda(s.ev_to_ebitda)},
      {label:'PE %ile 5Y', value:K.isNum(s.pe_percentile_5y)?K.fmtPct(s.pe_percentile_5y,0):K.NA, sub:'percentile storico', barW:(s.pe_percentile_5y||0)*100, col:_colForPct(s.pe_percentile_5y)},
      {label:'FCF Yield', value:K.fmtPct(s.fcf_yield,2), sub:'', barW:_scaleFcfYield(s.fcf_yield), col:_colForFcfYield(s.fcf_yield)},
      {label:'Div Yield', value:K.fmtPct(s.dividend_yield,2), sub:K.isNum(s.payout_ratio)?`Payout ${K.fmtPct(s.payout_ratio,0)}`:'', barW:(s.dividend_yield||0)*100*8, col:'var(--text-muted)'},
    ];

    container.innerHTML = cards.map(c => `
      <div class="snap-card">
        <div class="snap-label">${c.label}</div>
        <div class="snap-value">${c.value}</div>
        <div class="snap-sub">${c.sub || '&nbsp;'}</div>
        <div class="snap-bar"><div class="snap-bar-fill" style="width:${Math.max(0,Math.min(100,c.barW||0))}%;background:${c.col}"></div></div>
      </div>`).join('');
  }

  function _scaleOther(v, min, max){
    if(!K.isNum(v)) return 0;
    return Math.max(0, Math.min(100, ((v - min)/(max - min))*100));
  }
  function _scaleFcfYield(v){
    if(!K.isNum(v)) return 0;
    return Math.max(0, Math.min(100, v*100*15));
  }
  function _colForPe(v){
    if(!K.isNum(v)) return 'var(--text-muted)';
    if(v < 15) return 'var(--positive)';
    if(v < 25) return 'var(--neutral)';
    return 'var(--negative)';
  }
  function _colForEvEbitda(v){
    if(!K.isNum(v)) return 'var(--text-muted)';
    if(v < 10) return 'var(--positive)';
    if(v < 18) return 'var(--neutral)';
    return 'var(--negative)';
  }
  function _colForPeg(v){
    const f = K.flagPeg(v);
    return f==='good'?'var(--positive)':(f==='warn'?'var(--neutral)':f==='bad'?'var(--negative)':'var(--text-muted)');
  }
  function _colForPct(v){
    const f = K.flagPePercentile(v);
    return f==='good'?'var(--positive)':(f==='warn'?'var(--neutral)':f==='bad'?'var(--negative)':'var(--text-muted)');
  }
  function _colForFcfYield(v){
    if(!K.isNum(v)) return 'var(--text-muted)';
    if(v > 0.05) return 'var(--positive)';
    if(v > 0)    return 'var(--neutral)';
    return 'var(--negative)';
  }

  /* ---- CASH & DEBT ---------------------------------------------------- */
  function renderCashDebt(payload){
    const cd = payload.cash_debt || {};
    document.getElementById('cdCash').textContent = K.fmtMarketCap(cd.total_cash);
    document.getElementById('cdDebt').textContent = K.fmtMarketCap(cd.total_debt);
    document.getElementById('cdNet').textContent  = K.fmtMarketCap(cd.net_debt);

    const ratios = [
      {key:'net_debt_ebitda', name:'Net Debt / EBITDA',
       fmt:v=>K.fmtRatio(v,2), flag:K.flagNetDebtEbitda,
       desc:'<b>Cosa misura:</b> anni per ripagare il debito netto con l\'EBITDA.<br><b>Come leggerla:</b> &lt;2 sano · 2-4 warning · &gt;4 rischio.'},
      {key:'net_debt_fcf', name:'Net Debt / FCF',
       fmt:v=>K.fmtRatio(v,2), flag:K.flagNetDebtEbitda,
       desc:'<b>Cosa misura:</b> anni di free cash flow necessari per estinguere il debito.<br><b>Come leggerla:</b> &lt;3 eccellente · 3-7 ok · &gt;7 problematico.'},
      {key:'interest_coverage', name:'Interest Coverage',
       fmt:v=>K.fmtRatio(v,1), flag:K.flagInterestCoverage,
       desc:'<b>Cosa misura:</b> quante volte l\'EBIT copre gli interessi passivi.<br><b>Come leggerla:</b> &gt;5 solida · 2-5 vulnerabile · &lt;2 rischio.'},
      {key:'debt_equity', name:'Debt / Equity',
       fmt:v=>K.fmtRatio(v,2), flag:(v)=>!K.isNum(v)?'na':(v<0.5?'good':v<2?'warn':'bad'),
       desc:'<b>Cosa misura:</b> leva finanziaria (debito / patrimonio netto).<br><b>Come leggerla:</b> &lt;0.5 conservativo · 0.5-2 equilibrato · &gt;2 aggressivo.'},
      {key:'current_ratio', name:'Current Ratio',
       fmt:v=>K.fmtRatio(v,2), flag:K.flagCurrentRatio,
       desc:'<b>Cosa misura:</b> attivi correnti / passivi correnti.<br><b>Come leggerla:</b> &gt;1.5 forte · 1-1.5 ok · &lt;1 attenzione.'},
      {key:'quick_ratio', name:'Quick Ratio',
       fmt:v=>K.fmtRatio(v,2), flag:K.flagQuickRatio,
       desc:'<b>Cosa misura:</b> current ratio escludendo il magazzino.<br><b>Come leggerla:</b> &gt;1 liquidità solida · &lt;1 attenzione.'},
      {key:'cash_ratio', name:'Cash Ratio',
       fmt:v=>K.fmtRatio(v,2), flag:K.flagCashRatio,
       desc:'<b>Cosa misura:</b> cassa pura / passivi correnti. Il test più severo.<br><b>Come leggerla:</b> &gt;0.5 abbondante · 0.2-0.5 ok · &lt;0.2 stretta.'},
    ];

    const host = document.getElementById('cashDebtGrid');
    host.innerHTML = ratios.map(r => {
      const v = cd[r.key];
      const flag = r.flag(v);
      const flagLbl = K.labelFlag(flag);
      return `
        <div class="metric-card">
          <div class="metric-header">
            <span class="metric-name">${r.name}</span>
            <span class="metric-flag ${flag}">${flagLbl}</span>
          </div>
          <div class="metric-value mono">${r.fmt(v)}</div>
          <div class="metric-desc">${r.desc}</div>
        </div>`;
    }).join('');

    /* Trend net-debt 5Y */
    const trend = cd.net_debt_trend_5y || [];
    const ctx = document.getElementById('netDebtTrend');
    if(ctx && trend.length){
      new Chart(ctx.getContext('2d'),{
        type:'bar',
        data:{
          labels: trend.map(r => r.year),
          datasets:[{
            data: trend.map(r => (r.net_debt || 0) / 1e9),
            backgroundColor:'rgba(59,130,246,.7)',
            borderRadius:4,
          }],
        },
        options:{
          responsive:true, maintainAspectRatio:false,
          plugins:{legend:{display:false},tooltip:{
            backgroundColor:'#1a2235',titleColor:'#f1f5f9',bodyColor:'#cbd5e1',
            borderColor:'#2a3550',borderWidth:1,padding:10,displayColors:false,
            callbacks:{label:c=>'$'+c.parsed.y.toFixed(1)+'B'},
          }},
          scales:{x:{ticks:{color:'#64748b',font:{size:9}},grid:{display:false}},y:{display:false}},
        },
      });
    }
  }

  /* ---- CASHFLOW QUALITY ---------------------------------------------- */
  function renderCashflowQuality(payload){
    const q = payload.cashflow_quality || {};
    const items = [
      {name:'OCF / Net Income', val:K.fmtRatio(q.ocf_ni,2),
       flag:!K.isNum(q.ocf_ni)?'na':(q.ocf_ni>1?'good':q.ocf_ni>=0.8?'warn':'bad'),
       flagLbl:null,
       desc:'<b>Cosa misura:</b> cassa operativa per ogni $1 di utile netto.<br><b>Come leggerla:</b> &gt;1 utili di qualità · &lt;0.8 alert su accounting.'},
      {name:'Free Cash Flow TTM', val:K.fmtMarketCap(q.fcf_ttm),
       flag:!K.isNum(q.fcf_ttm)?'na':(q.fcf_ttm>0?'good':'bad'),
       flagLbl:null,
       desc:'<b>Cosa misura:</b> cassa residua dopo gli investimenti (CFO − CapEx). Disponibile per dividendi, buyback, M&amp;A, debito.'},
      {name:'FCF Margin', val:K.fmtPct(q.fcf_margin,2),
       flag:!K.isNum(q.fcf_margin)?'na':(q.fcf_margin>0.15?'good':q.fcf_margin>=0.05?'warn':'bad'),
       flagLbl:null,
       desc:'<b>Cosa misura:</b> FCF / ricavi. Efficienza nel convertire vendite in cassa.<br><b>Come leggerla:</b> &gt;15% business di altissima qualità.'},
      {name:'CapEx / Revenue', val:K.fmtPct(q.capex_rev,2),
       flag:!K.isNum(q.capex_rev)?'na':(q.capex_rev<0.05?'good':q.capex_rev<0.15?'warn':'bad'),
       flagLbl:null,
       desc:'<b>Cosa misura:</b> investimenti in capex come % dei ricavi.<br><b>Come leggerla:</b> &lt;5% asset-light · 5-15% medio · &gt;15% capital-intensive.'},
    ];
    document.getElementById('cashflowQualityGrid').innerHTML =
      items.map(i => _metricCardHtml(i)).join('');
  }

  function _metricCardHtml(i){
    const lbl = i.flagLbl || K.labelFlag(i.flag);
    return `
      <div class="metric-card">
        <div class="metric-header">
          <span class="metric-name">${i.name}</span>
          <span class="metric-flag ${i.flag}">${lbl}</span>
        </div>
        <div class="metric-value mono">${i.val}</div>
        <div class="metric-desc">${i.desc}</div>
      </div>`;
  }

  /* ---- PROFITABILITY -------------------------------------------------- */
  function renderProfitability(payload){
    const p = payload.profitability || {};
    const items = [
      {name:'ROE', val:K.fmtPct(p.roe,1),
       flag:!K.isNum(p.roe)?'na':(p.roe>0.15?'good':p.roe>=0.08?'warn':'bad'),
       desc:'<b>Cosa misura:</b> utile netto / patrimonio netto.<br><b>Come leggerla:</b> &gt;15% forte · &gt;25% eccellente (spesso da leva).'},
      {name:'ROA', val:K.fmtPct(p.roa,1),
       flag:!K.isNum(p.roa)?'na':(p.roa>0.05?'good':p.roa>=0.02?'warn':'bad'),
       desc:'<b>Cosa misura:</b> utile netto / totale attivo. Efficienza di tutti gli asset.'},
      {name:'ROIC TTM', val:K.fmtPct(p.roic_ttm ?? p.roic,1),
       flag:K.flagRoic(p.roic_ttm ?? p.roic),
       desc:'<b>Cosa misura:</b> ritorno sul capitale investito (equity + debito).<br><b>Come leggerla:</b> deve superare il WACC. ROIC−WACC è il vero valore creato.'},
      {name:'Gross Margin', val:K.fmtPct(p.gross_margin,1),
       flag:!K.isNum(p.gross_margin)?'na':(p.gross_margin>0.4?'good':p.gross_margin>=0.2?'warn':'bad'),
       desc:'<b>Cosa misura:</b> (ricavi − costi diretti) / ricavi. Pricing power.<br><b>Come leggerla:</b> trend crescente = moat in rafforzamento.'},
    ];
    document.getElementById('profitabilityGrid').innerHTML =
      items.map(i => _metricCardHtml(i)).join('');
  }

  /* ---- PRICE CHART (Lightweight Charts) ------------------------------- */
  function renderPriceChart(payload){
    const ps = payload.price_series || {};
    const priceContainer = document.getElementById('priceChart');
    const volContainer   = document.getElementById('volumeChart');
    const zContainer     = document.getElementById('zscoreChart');

    if(!priceContainer || !volContainer || !zContainer) return;
    if(!LW){ priceContainer.textContent = 'Lightweight Charts non caricato.'; return; }

    const candles = ps.candles || [];
    const volume  = ps.volume  || [];
    const sma50   = ps.sma50   || [];
    const sma200  = ps.sma200  || [];
    const zscore  = ps.zscore  || [];
    const earn    = ps.earnings|| [];

    if(candles.length === 0){
      priceContainer.innerHTML = '<div class="loading">Serie prezzi non disponibile.</div>';
      return;
    }

    /* Main price chart */
    const priceChart = LW.createChart(priceContainer,{
      layout:{background:{type:'solid',color:'#0a0e1a'},textColor:'#cbd5e1',fontFamily:'Inter',fontSize:11},
      grid:{vertLines:{color:'rgba(42,53,80,.4)'},horzLines:{color:'rgba(42,53,80,.4)'}},
      crosshair:{mode:LW.CrosshairMode.Normal,vertLine:{color:'#3b82f6',width:1,style:3,labelBackgroundColor:'#3b82f6'},horzLine:{color:'#3b82f6',width:1,style:3,labelBackgroundColor:'#3b82f6'}},
      rightPriceScale:{borderColor:'#2a3550',textColor:'#94a3b8'},
      timeScale:{borderColor:'#2a3550',textColor:'#94a3b8',timeVisible:false,secondsVisible:false},
      width:priceContainer.clientWidth,height:440,
    });
    const candleSeries = priceChart.addCandlestickSeries({
      upColor:'#10b981',downColor:'#ef4444',
      borderUpColor:'#10b981',borderDownColor:'#ef4444',
      wickUpColor:'#10b981',wickDownColor:'#ef4444',
    });
    candleSeries.setData(candles);
    if(sma50.length){
      const s1 = priceChart.addLineSeries({color:'#f59e0b',lineWidth:2,title:'SMA 50',priceLineVisible:false,lastValueVisible:false});
      s1.setData(sma50);
    }
    if(sma200.length){
      const s2 = priceChart.addLineSeries({color:'#8b5cf6',lineWidth:2,title:'SMA 200',priceLineVisible:false,lastValueVisible:false});
      s2.setData(sma200);
    }
    if(earn.length){
      candleSeries.setMarkers(earn.map(e => ({
        time: e.time, position:'belowBar', color:'#06b6d4',
        shape:'arrowUp', text:'E',
      })));
    }

    /* Volume chart */
    const volChart = LW.createChart(volContainer,{
      layout:{background:{type:'solid',color:'#0a0e1a'},textColor:'#cbd5e1',fontFamily:'Inter',fontSize:10},
      grid:{vertLines:{color:'rgba(42,53,80,.3)'},horzLines:{color:'rgba(42,53,80,.3)'}},
      rightPriceScale:{borderColor:'#2a3550',textColor:'#94a3b8'},
      timeScale:{borderColor:'#2a3550',visible:false},
      width:volContainer.clientWidth,height:100,
    });
    const volSeries = volChart.addHistogramSeries({priceFormat:{type:'volume'},priceScaleId:''});
    volSeries.priceScale().applyOptions({scaleMargins:{top:0.1,bottom:0}});
    volSeries.setData(volume);

    /* Z-score chart */
    const zChart = LW.createChart(zContainer,{
      layout:{background:{type:'solid',color:'#0a0e1a'},textColor:'#cbd5e1',fontFamily:'Inter',fontSize:10},
      grid:{vertLines:{color:'rgba(42,53,80,.3)'},horzLines:{color:'rgba(42,53,80,.3)'}},
      rightPriceScale:{borderColor:'#2a3550',textColor:'#94a3b8'},
      timeScale:{borderColor:'#2a3550',textColor:'#94a3b8'},
      width:zContainer.clientWidth,height:140,
    });
    const zSeries = zChart.addLineSeries({color:'#06b6d4',lineWidth:1.5,priceLineVisible:false});
    zSeries.setData(zscore);
    if(zscore.length){
      const mkLine = (v,col,style)=> {
        const s = zChart.addLineSeries({color:col,lineWidth:1,lineStyle:style,priceLineVisible:false,lastValueVisible:false});
        s.setData(zscore.map(p => ({time:p.time,value:v})));
        return s;
      };
      mkLine( 2, 'rgba(239,68,68,.5)', 2);
      mkLine(-2, 'rgba(239,68,68,.5)', 2);
      mkLine( 0, 'rgba(148,163,184,.3)', 2);
    }

    /* Sync time scales */
    function sync(source, targets){
      source.timeScale().subscribeVisibleLogicalRangeChange(range => {
        targets.forEach(t => t.timeScale().setVisibleLogicalRange(range));
      });
    }
    sync(priceChart,[volChart,zChart]);
    sync(volChart,[priceChart,zChart]);
    sync(zChart,[priceChart,volChart]);
    priceChart.timeScale().fitContent();

    window.addEventListener('resize', () => {
      priceChart.applyOptions({width:priceContainer.clientWidth});
      volChart.applyOptions({width:volContainer.clientWidth});
      zChart.applyOptions({width:zContainer.clientWidth});
    });
  }

  /* ---- DIAGNOSTICS (3 mini-chart Chart.js) --------------------------- */
  function renderDiagnostics(payload){
    const d = payload.diagnostics || {};
    const t = payload.technical || {};

    _setDiagTitle('ddTitle', 'Drawdown from ATH', K.fmtPct(t.drawdown_from_ath,1));
    _setDiagTitle('hvTitle', 'Historical Volatility 20 (annualizzata)', K.fmtPct(t.hv_20_annualized,1));
    _setDiagTitle('rsTitle', 'Relative Strength vs benchmark (rolling 6m)',
      K.isNum(t.rs_126d) ? K.fmtPctSigned(t.rs_126d,1) : K.NA);

    _chartLine('ddChart', d.drawdown, '#ef4444', v => K.fmtPct(v,2), {yMax:0});
    _chartLine('hvChart', d.hv_20,    '#f59e0b', v => K.fmtPct(v,1));
    _chartLine('rsChart', d.rs_126d,  '#06b6d4', v => K.fmtPctSigned(v,1));
  }

  function _setDiagTitle(id, name, val){
    const el = document.getElementById(id);
    if(!el) return;
    el.innerHTML = `<span>${name}</span><span class="val">${val}</span>`;
  }

  function _chartLine(canvasId, series, color, tickFmt, opts){
    opts = opts || {};
    const el = document.getElementById(canvasId);
    if(!el || !series || !series.length) return;
    const labels = series.map(p => new Date(p.time*1000).toISOString().slice(0,7));
    const data = series.map(p => p.value != null ? +(p.value*100).toFixed(2) : null);

    new Chart(el.getContext('2d'),{
      type:'line',
      data:{labels, datasets:[{
        data,
        borderColor:color,
        backgroundColor: color.startsWith('#ef44')?'rgba(239,68,68,.15)':
                          color.startsWith('#f59e')?'rgba(245,158,11,.15)':
                                                     'rgba(6,182,212,.15)',
        fill:true,borderWidth:1.5,pointRadius:0,tension:.2,
      }]},
      options:{
        responsive:true, maintainAspectRatio:false,
        plugins:{legend:{display:false},tooltip:{
          backgroundColor:'#1a2235',titleColor:'#f1f5f9',bodyColor:'#cbd5e1',
          borderColor:'#2a3550',borderWidth:1,padding:10,displayColors:false,
        }},
        scales:{
          x:{ticks:{color:'#64748b',maxTicksLimit:6},grid:{color:'rgba(42,53,80,.25)'}},
          y:{ticks:{color:'#64748b',callback:v=>v+'%'},grid:{color:'rgba(42,53,80,.25)'},
              max: opts.yMax!==undefined ? opts.yMax : undefined},
        },
      },
    });
  }

  /* ---- SEASONALITY line chart ---------------------------------------- */
  function renderSeasonality(payload){
    const canvas = document.getElementById('seasonalityChart');
    const metaHost = document.getElementById('seasonalityMeta');
    const s = payload.seasonality;
    if(!canvas) return;
    if(!s){
      canvas.parentElement.innerHTML =
        '<div class="loading">Dati stagionalità non disponibili.</div>';
      if(metaHost) metaHost.innerHTML = '';
      return;
    }

    const months = s.months || ['Gen','Feb','Mar','Apr','Mag','Giu','Lug','Ago','Set','Ott','Nov','Dic'];
    /* Preferiamo i nuovi campi; fallback al vecchio matrix[AVG] se assenti. */
    let avg = s.monthly_avg;
    let det = s.monthly_avg_detrended;
    const hit = s.monthly_hit_rate || [];

    if(!avg){
      /* backward compat: l'ultima riga di matrix è AVG */
      const mat = s.matrix || [];
      if(mat.length){
        avg = mat[mat.length - 1];
      }
    }
    if(!avg){ return; }

    /* Se manca detrended (JSON vecchio), lo calcoliamo lato client */
    if(!det){
      const nonNull = avg.filter(v => v !== null && v !== undefined);
      const mean = nonNull.length ? nonNull.reduce((a,b)=>a+b,0)/nonNull.length : 0;
      det = avg.map(v => (v === null || v === undefined) ? null : v - mean);
    }

    /* Converti in % per leggibilità chart */
    const avgPct = avg.map(v => v === null || v === undefined ? null : +(v*100).toFixed(2));
    const detPct = det.map(v => v === null || v === undefined ? null : +(v*100).toFixed(2));
    const hitPct = hit.map(v => v === null || v === undefined ? null : +(v*100).toFixed(1));

    /* Colori barre hit rate: verdi se >50, rossi se <50 */
    const hitColors = hitPct.map(v => {
      if(v === null) return 'rgba(100,116,139,.15)';
      return v >= 50 ? 'rgba(16,185,129,.28)' : 'rgba(239,68,68,.28)';
    });

    /* ---- bullet summary in meta -------------------------------------- */
    if(metaHost){
      const nYears = s.n_years || (s.years ? s.years.length - 1 : null);
      /* trova mese migliore / peggiore dalla detrendizzata */
      let bestIdx = -1, worstIdx = -1;
      let bestV = -Infinity, worstV = +Infinity;
      detPct.forEach((v, i) => {
        if(v === null) return;
        if(v > bestV){ bestV = v; bestIdx = i; }
        if(v < worstV){ worstV = v; worstIdx = i; }
      });
      const chips = [];
      if(nYears) chips.push(`<span class="chip"><b>${nYears}</b> anni di storia</span>`);
      if(bestIdx >= 0) chips.push(`<span class="chip">Mese migliore: <b>${months[bestIdx]}</b> (<span class="cell-pos">${bestV>=0?'+':''}${bestV.toFixed(2)}%</span> vs media)</span>`);
      if(worstIdx >= 0) chips.push(`<span class="chip">Mese peggiore: <b>${months[worstIdx]}</b> (<span class="cell-neg">${worstV.toFixed(2)}%</span> vs media)</span>`);
      if(K.isNum(s.overall_mean)) chips.push(`<span class="chip">Drift mensile medio: <b>${s.overall_mean>=0?'+':''}${(s.overall_mean*100).toFixed(2)}%</b></span>`);
      metaHost.innerHTML = chips.join('');
    }

    /* ---- Chart.js ---------------------------------------------------- */
    new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: months,
        datasets: [
          {
            type: 'bar',
            label: 'Hit rate (% anni positivi)',
            data: hitPct,
            backgroundColor: hitColors,
            borderColor: 'transparent',
            yAxisID: 'yHit',
            order: 3,
            borderRadius: 2,
          },
          {
            type: 'line',
            label: 'Media mensile',
            data: avgPct,
            borderColor: '#06b6d4',
            backgroundColor: 'rgba(6,182,212,.12)',
            borderWidth: 2.2,
            pointRadius: 3.5,
            pointHoverRadius: 5,
            pointBackgroundColor: '#06b6d4',
            tension: .3,
            yAxisID: 'yRet',
            order: 1,
          },
          {
            type: 'line',
            label: 'Detrendizzata (stagionalità pura)',
            data: detPct,
            borderColor: '#f59e0b',
            backgroundColor: 'rgba(245,158,11,.06)',
            borderDash: [6,4],
            borderWidth: 2,
            pointRadius: 3,
            pointHoverRadius: 5,
            pointBackgroundColor: '#f59e0b',
            tension: .3,
            yAxisID: 'yRet',
            order: 2,
            fill: false,
          },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            display: true,
            labels: { color: '#cbd5e1', font: { size: 11 }, usePointStyle: true, padding: 14 },
          },
          tooltip: {
            backgroundColor:'#1a2235', titleColor:'#f1f5f9', bodyColor:'#cbd5e1',
            borderColor:'#2a3550', borderWidth:1, padding:10,
            callbacks: {
              label: (ctx) => {
                const v = ctx.parsed.y;
                if(v === null || v === undefined) return ctx.dataset.label + ': —';
                if(ctx.dataset.label && ctx.dataset.label.startsWith('Hit rate')){
                  return ' Hit rate: ' + v.toFixed(1) + '%';
                }
                return ' ' + ctx.dataset.label + ': ' + (v>=0?'+':'') + v.toFixed(2) + '%';
              },
            },
          },
        },
        scales: {
          x: {
            ticks: { color:'#cbd5e1', font:{ size: 11 } },
            grid:  { color: 'rgba(42,53,80,.3)' },
          },
          yRet: {
            position: 'left',
            ticks: { color:'#cbd5e1', callback: v => (v>=0?'+':'') + v + '%' },
            grid:  { color: 'rgba(42,53,80,.3)' },
            title: { display:true, text:'Return mensile (%)', color:'#64748b', font:{size:10}},
          },
          yHit: {
            position: 'right',
            min: 0, max: 100,
            ticks: { color:'#64748b', callback: v => v + '%' },
            grid:  { display: false },
            title: { display:true, text:'Hit rate (%)', color:'#64748b', font:{size:10}},
          },
        },
      },
    });
  }

  /* ---- SCORES --------------------------------------------------------- */
  function renderScores(payload){
    const s = payload.scores || {};
    const host = document.getElementById('scoresGrid');
    if(!host) return;

    const az = s.altman_z || {};
    const pf = s.piotroski_f || {};
    const bm = s.beneish_m || {};

    function azFrac(v){ if(!K.isNum(v)) return 0; return Math.min(Math.max(v/6, 0), 1); }
    function pfFrac(v){ if(!K.isNum(v)) return 0; return v/9; }
    function bmFrac(v){
      if(!K.isNum(v)) return 0;
      /* interpretiamo "più basso = meglio" → proiettiamo in [0,1] con -4 best, +2 worst */
      return Math.max(0, Math.min(1, ( -v + 2 ) / 6 ));
    }

    const azRate = az.rating || 'n/a';
    const pfRate = pf.rating || 'n/a';
    const bmRate = bm.rating || 'n/a';

    const cards = [
      {
        name:'Altman Z-Score',
        ringHtml: ringSvg(azFrac(az.value), scoreColorVar(azRate)),
        bigValue: K.fmtNumber(az.value, 2),
        subMax:   'safe &gt; 3.0',
        ratingCls: K.scoreRatingClass(azRate),
        ratingLbl: K.labelScoreRating(azRate),
        desc:'<b>Cosa misura:</b> probabilità di bancarotta nei prossimi 2 anni. Combina 5 ratios.<br><b>Scala:</b> &gt;3.0 safe · 1.8-3.0 grey · &lt;1.8 distress.',
      },
      {
        name:'Piotroski F-Score',
        ringHtml: ringSvg(pfFrac(pf.value), scoreColorVar(pfRate)),
        bigValue: K.isNum(pf.value) ? pf.value + '<span style="font-size:14px;color:var(--text-muted)">/9</span>' : K.NA,
        subMax:   'high ≥ 7',
        ratingCls: K.scoreRatingClass(pfRate),
        ratingLbl: K.labelScoreRating(pfRate),
        desc:'<b>Cosa misura:</b> qualità fondamentale in 9 test binari.<br><b>Scala:</b> 7-9 forte · 4-6 neutro · ≤3 debole.',
      },
      {
        name:'Beneish M-Score',
        ringHtml: ringSvg(bmFrac(bm.value), scoreColorVar(bmRate)),
        bigValue: K.fmtNumber(bm.value, 2),
        subMax:   'clean &lt; -2.22',
        ratingCls: K.scoreRatingClass(bmRate),
        ratingLbl: K.labelScoreRating(bmRate),
        desc:'<b>Cosa misura:</b> probabilità di manipolazione contabile degli utili.<br><b>Scala:</b> &lt;-2.22 clean · &gt;-2.22 alert.',
      },
    ];

    host.innerHTML = cards.map(c => `
      <div class="score-card">
        <div class="score-ring">
          ${c.ringHtml}
          <div class="score-ring-text">
            <div class="score-ring-value mono">${c.bigValue}</div>
            <div class="score-ring-max">${c.subMax}</div>
          </div>
        </div>
        <div class="score-name">${c.name}</div>
        <div class="score-rating ${c.ratingCls}">${c.ratingLbl}</div>
        <div class="score-desc">${c.desc}</div>
      </div>`).join('');
  }

  /* ---- KQ composite + rank ------------------------------------------- */
  function renderKqScore(payload){
    const k = payload.kq_score || {};
    const host = document.getElementById('kqScoreBox');
    if(!host) return;

    const v = k.value;
    const cls = K.flagKqScore(v);
    const rg = K.isNum(k.rank_global) ? '#' + k.rank_global : K.NA;
    const rs = K.isNum(k.rank_sector) ? '#' + k.rank_sector : K.NA;

    host.innerHTML = `
      <div class="interpret" style="margin-top:0">
        <h4>KQ Value Score · sintesi di 4 dimensioni</h4>
        <p>
          Il titolo ha un KQ Value Score di
          <span class="highlight">${K.isNum(v) ? K.fmtNumber(v,1) : K.NA}/100</span>
          (<span class="score-pill s-${cls}">${K.labelFlag(cls)}</span>).
          Ranking globale: <b>${rg}</b> · Ranking settoriale: <b>${rs}</b>.
        </p>
        <p style="font-size:12px;color:var(--text-muted)">
          Il punteggio aggrega quattro dimensioni con i seguenti pesi:
          <b>Valutazione 40%</b> (PE, Forward PE, PEG, percentile 5Y) ·
          <b>Qualità 25%</b> (ROIC, FCF margin, Piotroski F) ·
          <b>Solidità 20%</b> (Altman Z, interest coverage, current ratio, net debt / EBITDA) ·
          <b>Momentum 15%</b> (RS 6m, prezzo vs SMA 200).
          Tutte le componenti sono normalizzate via <em>rank percentile</em> e ribaltate quando "basso = meglio".
        </p>
      </div>`;
  }

  /* ---- AI read -------------------------------------------------------- */
  function renderAiRead(payload){
    const a = payload.ai_read || {};
    const host = document.getElementById('aiReadBox');
    if(!host) return;
    const bullets = (a.bullets || []).map(b => `<li>${b}</li>`).join('');
    host.innerHTML = `
      <div class="interpret">
        <h4>${payload.meta?.ticker || ''} in sintesi · lettura algoritmica</h4>
        <p>${a.summary || ''}</p>
        ${bullets ? `<ul>${bullets}</ul>` : ''}
        <p style="color:var(--text-muted);font-size:12px;margin-top:14px;padding-top:14px;border-top:1px solid var(--border-soft)">
          ${a.disclaimer || ''}
        </p>
      </div>`;
  }

  /* ---- BOOT ----------------------------------------------------------- */
  async function boot(){
    const ticker = getTickerParam();
    const err = document.getElementById('errorBox');
    const loading = document.getElementById('loadingBox');

    if(!ticker){
      loading.style.display = 'none';
      err.style.display = 'block';
      err.textContent = 'Parametro ?t=TICKER mancante. Torna allo screener.';
      return;
    }

    const url = `data/tickers/${encodeURIComponent(ticker)}.json`;
    try{
      const payload = await K.loadJson(url);
      loading.style.display = 'none';
      document.getElementById('pageBody').style.display = '';

      renderHero(payload);
      renderSnapshot(payload);
      renderCashDebt(payload);
      renderCashflowQuality(payload);
      renderProfitability(payload);
      renderPriceChart(payload);
      renderDiagnostics(payload);
      renderSeasonality(payload);
      renderScores(payload);
      renderKqScore(payload);
      renderAiRead(payload);

    }catch(ex){
      console.error(ex);
      loading.style.display = 'none';
      err.style.display = 'block';
      err.textContent = `Errore caricamento dati ticker "${ticker}" · ${ex.message || ex}`;
    }
  }

  document.addEventListener('DOMContentLoaded', boot);
})();
