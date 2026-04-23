/* ============================================================================
 * KQ Value Scanner · formatters.js
 * ---------------------------------------------------------------------------
 * Helper condivisi per formattazione numeri, percentuali, market-cap, colori
 * delle flag in base alle soglie di Kriterion Quant.
 *
 * Esposto come globale `window.KQ` (pattern compat senza bundler).
 * ========================================================================== */
(function(){
  'use strict';

  const NA = '—';

  function isNum(v){
    return typeof v === 'number' && !Number.isNaN(v) && Number.isFinite(v);
  }

  /* ---- Number formatters ----------------------------------------------- */
  function fmtNumber(v, digits){
    if(!isNum(v)) return NA;
    return v.toLocaleString('it-IT', {
      minimumFractionDigits: digits ?? 2,
      maximumFractionDigits: digits ?? 2,
    });
  }

  function fmtRatio(v, digits){
    if(!isNum(v)) return NA;
    return fmtNumber(v, digits ?? 2) + 'x';
  }

  function fmtPct(v, digits){
    if(!isNum(v)) return NA;
    return (v * 100).toLocaleString('it-IT', {
      minimumFractionDigits: digits ?? 2,
      maximumFractionDigits: digits ?? 2,
    }) + '%';
  }

  function fmtPctSigned(v, digits){
    if(!isNum(v)) return NA;
    const s = fmtPct(v, digits);
    return v > 0 ? '+' + s : s;
  }

  function fmtMoney(v, currency){
    if(!isNum(v)) return NA;
    const abs = Math.abs(v);
    const sign = v < 0 ? '-' : '';
    const sym = currency === 'USD' ? '$' : (currency ? currency + ' ' : '$');
    if(abs >= 1e12) return sign + sym + (abs/1e12).toFixed(2) + 'T';
    if(abs >= 1e9)  return sign + sym + (abs/1e9).toFixed(2)  + 'B';
    if(abs >= 1e6)  return sign + sym + (abs/1e6).toFixed(1)  + 'M';
    if(abs >= 1e3)  return sign + sym + (abs/1e3).toFixed(1)  + 'K';
    return sign + sym + abs.toFixed(2);
  }

  function fmtMarketCap(v){ return fmtMoney(v, 'USD'); }

  function fmtInt(v){
    if(!isNum(v)) return NA;
    return Math.round(v).toLocaleString('it-IT');
  }

  function fmtDate(iso){
    if(!iso) return NA;
    try{
      const d = new Date(iso);
      if(isNaN(d.getTime())) return iso;
      return d.toLocaleDateString('it-IT',
        {day:'numeric', month:'long', year:'numeric'});
    }catch(_){ return iso; }
  }

  /* ---- Color / flag helpers -------------------------------------------- */
  function signClass(v){
    if(!isNum(v)) return 'cell-muted';
    if(v > 0) return 'cell-pos';
    if(v < 0) return 'cell-neg';
    return 'cell-muted';
  }

  // Net Debt / EBITDA: <2 sano, 2-4 warn, >4 bad
  function flagNetDebtEbitda(v){
    if(!isNum(v)) return 'na';
    if(v < 2.0) return 'good';
    if(v < 4.0) return 'warn';
    return 'bad';
  }

  // Interest coverage: >5 sano, 2-5 warn, <2 bad
  function flagInterestCoverage(v){
    if(!isNum(v)) return 'na';
    if(v > 5.0) return 'good';
    if(v >= 2.0) return 'warn';
    return 'bad';
  }

  // Current ratio: >1.5 sano, 1-1.5 warn, <1 bad
  function flagCurrentRatio(v){
    if(!isNum(v)) return 'na';
    if(v > 1.5) return 'good';
    if(v >= 1.0) return 'warn';
    return 'bad';
  }

  // Quick ratio: >1 sano, 0.5-1 warn, <0.5 bad
  function flagQuickRatio(v){
    if(!isNum(v)) return 'na';
    if(v > 1.0) return 'good';
    if(v >= 0.5) return 'warn';
    return 'bad';
  }

  // Cash ratio: >0.5 sano, 0.2-0.5 warn, <0.2 bad
  function flagCashRatio(v){
    if(!isNum(v)) return 'na';
    if(v > 0.5) return 'good';
    if(v >= 0.2) return 'warn';
    return 'bad';
  }

  // PEG: <1 good, 1-2 warn, >2 bad
  function flagPeg(v){
    if(!isNum(v)) return 'na';
    if(v < 1.0) return 'good';
    if(v < 2.0) return 'warn';
    return 'bad';
  }

  // PE percentile 5Y: <0.3 good, 0.3-0.7 warn, >0.7 bad
  function flagPePercentile(v){
    if(!isNum(v)) return 'na';
    if(v < 0.3) return 'good';
    if(v < 0.7) return 'warn';
    return 'bad';
  }

  // KQ Value Score 0-100: >=70 strong, 40-70 mid, <40 weak
  function flagKqScore(v){
    if(!isNum(v)) return 'weak';
    if(v >= 70) return 'strong';
    if(v >= 40) return 'mid';
    return 'weak';
  }

  // ROIC %: >0.15 good, 0.08-0.15 warn, <0.08 bad
  function flagRoic(v){
    if(!isNum(v)) return 'na';
    if(v > 0.15) return 'good';
    if(v >= 0.08) return 'warn';
    return 'bad';
  }

  // Altman Z: >=3 safe, 1.8-3 grey, <1.8 distress
  function flagAltmanZ(v){
    if(!isNum(v)) return 'na';
    if(v >= 3.0) return 'good';
    if(v >= 1.8) return 'warn';
    return 'bad';
  }

  // Piotroski F (intero 0-9): >=7 good, 4-6 warn, <=3 bad
  function flagPiotroski(v){
    if(!isNum(v)) return 'na';
    if(v >= 7) return 'good';
    if(v >= 4) return 'warn';
    return 'bad';
  }

  // Beneish M: <-2.22 clean, >-2.22 alert
  function flagBeneish(v){
    if(!isNum(v)) return 'na';
    return v < -2.22 ? 'good' : 'warn';
  }

  /* ---- Human labels ---------------------------------------------------- */
  function labelFlag(flag){
    switch(flag){
      case 'good':   return 'SANO';
      case 'warn':   return 'ATTENZIONE';
      case 'bad':    return 'RISCHIO';
      case 'strong': return 'FORTE';
      case 'mid':    return 'MEDIO';
      case 'weak':   return 'DEBOLE';
      case 'na':     return 'N/A';
      default:       return '';
    }
  }

  function labelScoreRating(rating){
    switch(rating){
      case 'safe':     return 'ZONA SICURA';
      case 'grey':     return 'ZONA GRIGIA';
      case 'distress': return 'DISTRESS';
      case 'strong':   return 'QUALITÀ ALTA';
      case 'neutral':  return 'NEUTRA';
      case 'weak':     return 'QUALITÀ BASSA';
      case 'clean':    return 'NON MANIPOLATORE';
      case 'alert':    return 'ALERT';
      default:         return 'N/A';
    }
  }

  function scoreRatingClass(rating){
    if(['safe','strong','clean'].includes(rating)) return 'strong';
    if(['grey','neutral'].includes(rating)) return 'neutral';
    if(['distress','weak','alert'].includes(rating)) return 'weak';
    return 'neutral';
  }

  /* ---- Data loader ----------------------------------------------------- */
  async function loadJson(url){
    const r = await fetch(url, {cache:'no-store'});
    if(!r.ok) throw new Error('HTTP ' + r.status + ' · ' + url);
    return r.json();
  }

  /* ---- Export ---------------------------------------------------------- */
  window.KQ = {
    NA,
    isNum,
    fmtNumber, fmtRatio, fmtPct, fmtPctSigned,
    fmtMoney, fmtMarketCap, fmtInt, fmtDate,
    signClass,
    flagNetDebtEbitda, flagInterestCoverage,
    flagCurrentRatio,  flagQuickRatio, flagCashRatio,
    flagPeg, flagPePercentile,
    flagKqScore, flagRoic,
    flagAltmanZ, flagPiotroski, flagBeneish,
    labelFlag, labelScoreRating, scoreRatingClass,
    loadJson,
  };
})();
