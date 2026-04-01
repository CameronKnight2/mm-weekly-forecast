/**
 * Mid-Market Weekly Exec Comms: Trend Summary Deck Generator
 *
 * HOW TO RUN:
 *   1. Open your "Test Slides For Cursor" presentation in Google Slides
 *   2. Go to Extensions > Apps Script
 *   3. Delete any existing code in the editor
 *   4. Paste this entire file's contents
 *   5. Click the Run button (or Run > Run function > buildDeck)
 *   6. Authorize when prompted (first run only)
 *   7. Return to your Slides tab -- all 12 slides will be populated
 */

function buildDeck() {
  var pres = SlidesApp.getActivePresentation();

  // Remove all existing slides
  while (pres.getSlides().length > 0) {
    pres.getSlides()[0].remove();
  }

  // Color constants
  var DARK_BLUE = '#1F2954';
  var WHITE = '#FFFFFF';
  var DARK_GRAY = '#404040';
  var MEDIUM_BLUE = '#3366B3';
  var LIGHT_GRAY_BG = '#EDEEF8';

  // Slide dimensions
  var W = pres.getPageWidth();
  var H = pres.getPageHeight();

  // ---------------------------------------------------------------------------
  // Helper functions
  // ---------------------------------------------------------------------------

  function addTitleSlide(titleText, subtitleText, preparedText) {
    var slide = pres.appendSlide(SlidesApp.PredefinedLayout.BLANK);
    slide.getBackground().setSolidFill(DARK_BLUE);

    var title = slide.insertTextBox(titleText, W * 0.05, H * 0.22, W * 0.9, H * 0.3);
    title.getText().getTextStyle()
      .setFontSize(36).setBold(true).setForegroundColor(WHITE).setFontFamily('Arial');
    title.getText().getParagraphStyle().setParagraphAlignment(SlidesApp.ParagraphAlignment.CENTER);

    var sub = slide.insertTextBox(subtitleText, W * 0.05, H * 0.55, W * 0.9, H * 0.08);
    sub.getText().getTextStyle()
      .setFontSize(20).setForegroundColor(WHITE).setFontFamily('Arial');
    sub.getText().getParagraphStyle().setParagraphAlignment(SlidesApp.ParagraphAlignment.CENTER);

    var prep = slide.insertTextBox(preparedText, W * 0.05, H * 0.68, W * 0.9, H * 0.06);
    prep.getText().getTextStyle()
      .setFontSize(14).setItalic(true).setForegroundColor(WHITE).setFontFamily('Arial');
    prep.getText().getParagraphStyle().setParagraphAlignment(SlidesApp.ParagraphAlignment.CENTER);

    return slide;
  }

  function addContentSlide(titleText, bodyText, boldPhrases) {
    var slide = pres.appendSlide(SlidesApp.PredefinedLayout.BLANK);

    // Title bar
    var bar = slide.insertShape(SlidesApp.ShapeType.RECTANGLE, 0, 0, W, H * 0.1);
    bar.getFill().setSolidFill(DARK_BLUE);
    bar.getBorder().setTransparent();
    var barText = bar.getText();
    barText.setText('  ' + titleText);
    barText.getTextStyle()
      .setFontSize(22).setBold(true).setForegroundColor(WHITE).setFontFamily('Arial');

    // Body
    var body = slide.insertTextBox(bodyText, W * 0.03, H * 0.13, W * 0.94, H * 0.84);
    body.getText().getTextStyle()
      .setFontSize(12).setForegroundColor(DARK_GRAY).setFontFamily('Arial');

    // Bold specific phrases
    if (boldPhrases && boldPhrases.length > 0) {
      var text = body.getText().asString();
      for (var i = 0; i < boldPhrases.length; i++) {
        var phrase = boldPhrases[i];
        var idx = text.indexOf(phrase);
        if (idx >= 0) {
          body.getText().getRange(idx, idx + phrase.length).getTextStyle().setBold(true);
        }
      }
    }

    return slide;
  }

  function addContentSlideWithTable(titleText, summaryText, headers, rows, boldPhrases) {
    var slide = pres.appendSlide(SlidesApp.PredefinedLayout.BLANK);

    // Title bar
    var bar = slide.insertShape(SlidesApp.ShapeType.RECTANGLE, 0, 0, W, H * 0.1);
    bar.getFill().setSolidFill(DARK_BLUE);
    bar.getBorder().setTransparent();
    bar.getText().setText('  ' + titleText);
    bar.getText().getTextStyle()
      .setFontSize(22).setBold(true).setForegroundColor(WHITE).setFontFamily('Arial');

    // Summary bullets
    var summary = slide.insertTextBox(summaryText, W * 0.03, H * 0.11, W * 0.94, H * 0.22);
    summary.getText().getTextStyle()
      .setFontSize(10).setForegroundColor(DARK_GRAY).setFontFamily('Arial');

    if (boldPhrases && boldPhrases.length > 0) {
      var sText = summary.getText().asString();
      for (var i = 0; i < boldPhrases.length; i++) {
        var p = boldPhrases[i];
        var si = sText.indexOf(p);
        if (si >= 0) {
          summary.getText().getRange(si, si + p.length).getTextStyle().setBold(true);
        }
      }
    }

    // Table
    var nRows = rows.length + 1;
    var nCols = headers.length;
    var table = slide.insertTable(nRows, nCols, W * 0.03, H * 0.34, W * 0.94, H * 0.62);

    // Header row
    for (var c = 0; c < nCols; c++) {
      var cell = table.getCell(0, c);
      cell.getText().setText(headers[c]);
      cell.getText().getTextStyle()
        .setFontSize(9).setBold(true).setForegroundColor(WHITE).setFontFamily('Arial');
      cell.getFill().setSolidFill(DARK_BLUE);
    }

    // Data rows
    for (var r = 0; r < rows.length; r++) {
      for (var c = 0; c < nCols; c++) {
        var cell = table.getCell(r + 1, c);
        cell.getText().setText(String(rows[r][c]));
        cell.getText().getTextStyle()
          .setFontSize(8).setForegroundColor(DARK_GRAY).setFontFamily('Arial');
        if (r % 2 === 1) {
          cell.getFill().setSolidFill(LIGHT_GRAY_BG);
        }
      }
    }

    return slide;
  }

  function addConclusionSlide(titleText, bodyText) {
    var slide = pres.appendSlide(SlidesApp.PredefinedLayout.BLANK);
    slide.getBackground().setSolidFill(DARK_BLUE);

    var title = slide.insertTextBox(titleText, W * 0.05, H * 0.05, W * 0.9, H * 0.12);
    title.getText().getTextStyle()
      .setFontSize(28).setBold(true).setForegroundColor(WHITE).setFontFamily('Arial');
    title.getText().getParagraphStyle().setParagraphAlignment(SlidesApp.ParagraphAlignment.CENTER);

    var body = slide.insertTextBox(bodyText, W * 0.08, H * 0.2, W * 0.84, H * 0.75);
    body.getText().getTextStyle()
      .setFontSize(16).setForegroundColor(WHITE).setFontFamily('Arial');

    return slide;
  }

  // ---------------------------------------------------------------------------
  // SLIDE 1: Title
  // ---------------------------------------------------------------------------
  addTitleSlide(
    'Mid-Market Weekly Exec Comms\nTrend Summary',
    'October 2025 \u2013 March 2026',
    'Prepared for Executive Leadership'
  );

  // ---------------------------------------------------------------------------
  // SLIDE 2: Executive Summary
  // ---------------------------------------------------------------------------
  addContentSlide(
    'Executive Summary',
    '\u2022  Q1 closed with record momentum (125 IES deals in final week, 839 IES deals for the quarter), ' +
    'establishing a strong foundation entering Q2.\n\n' +
    '\u2022  Q2 saw sustained acceleration \u2014 pipeline grew from $33M to $47M, win rates held 5\u201312 pts above ' +
    'target, and the quarter closed with a record 193 IES deals and $2.38M revenue in the final week.\n\n' +
    '\u2022  Q3 ramp followed historical early-quarter patterns (76\u201394 contracts in Wks 29\u201330), then inflected ' +
    'sharply: pipeline creation hit $7.2M (vs. $6.3M target) and IES mix improved from ~45% to ~65% of SQOs.\n\n' +
    '\u2022  Structural investments are compounding: BDR-to-AE promotions, Gong AI coaching, Orum dialer launch, ' +
    'franchise strategy (300+ IFA leads), and 6 Renewal Consultants expanding retention capacity.\n\n' +
    '\u2022  Key risks remain: post-sale support drag on seller time, demand gen structural gap, and ' +
    'Payments/QBO Advanced competitiveness in the 100+ employee segment.',
    ['125 IES deals', '839 IES deals', '$47M', '193 IES deals', '$2.38M',
     '$7.2M', '~65%', '300+ IFA leads', '6 Renewal Consultants']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 3: IES Contract Performance
  // ---------------------------------------------------------------------------
  addContentSlideWithTable(
    'IES Contract Performance Over Time',
    '\u2022  Weekly IES contracts ranged from 52 (holiday short week) to 193 (Q2 close record)\n' +
    '\u2022  Targets escalated from ~100/wk in Q2 to 143\u2013160/wk in Q3 as capacity scaled\n' +
    '\u2022  Q3 inflection: 76 \u2192 94 \u2192 144 \u2192 105 \u2192 129 across Wks 29\u201333, tracking upward\n' +
    '\u2022  BDR-to-AE promotions producing faster ramp \u2014 some new AEs closing within 2 weeks',
    ['Week', 'Date', 'IES Contracts', 'Target', 'Notes'],
    [
      ['Wk 14', '10/31/25', '125', '~100', 'Q1 close record week'],
      ['Wk 17', '11/21/25', '114', '~100', 'Strong Q2 acceleration'],
      ['Wk 21', '12/19/25', '130+', '~100', 'Pre-Recharge push'],
      ['Wk 24', '1/9/26', '103', '~100', 'New year ramp'],
      ['Wk 27', '1/30/26', '193', '~143', 'Q2 close record: 176+17 WSB'],
      ['Wk 29', '2/14/26', '76', '~103', 'Early Q3, historical ramp'],
      ['Wk 31', '2/28/26', '144', '143', 'Strong rebound, exceeded target'],
      ['Wk 32', '3/8/26', '105', '~143', '100 direct + 5 WSB'],
      ['Wk 33', '3/15/26', '129', '160', '118 direct + 11 WSB'],
    ],
    ['193', '144', '125']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 4: Revenue Trends
  // ---------------------------------------------------------------------------
  addContentSlideWithTable(
    'Revenue & Incremental Sales Trends',
    '\u2022  Weekly IES incremental revenue ranged from $0.6M (holiday) to $2.38M (Q2 close)\n' +
    '\u2022  Q3 steady-state trending $1.5\u2013$1.7M/wk with improving IES mix\n' +
    '\u2022  Signed-but-not-booked backlog: $1.8M \u2192 $1.2M \u2192 $1.51M \u2014 operational focus ongoing\n' +
    '\u2022  Payroll at 144% of quarterly target (822 units); Bill Pay at 113% to target',
    ['Week', 'Date', 'Total Revenue', 'Context'],
    [
      ['Wk 15', '11/7/25', '$1.1M', '59 IES'],
      ['Wk 17', '11/21/25', '$1.35M', '114 IES'],
      ['Wk 20', '12/14/25', '$1.62M', '117 IES - strongest non-EOQ'],
      ['Wk 25', '1/16/26', '$1.9M', 'Catch-up bookings (107 ITF)'],
      ['Wk 27', '1/30/26', '$2.38M', '193 IES - Q2 close'],
      ['Wk 30', '2/21/26', '$0.86M', '94 IES (58 ITF)'],
      ['Wk 31', '2/28/26', '$1.69M', '144 IES - exceeded target'],
      ['Wk 32', '3/8/26', '$1.52M', '100 direct + 5 WSB'],
      ['Wk 33', '3/15/26', '$1.69M', '129 IES (84 ITF)'],
    ],
    ['$2.38M', '$1.5\u2013$1.7M/wk', '144%', '113%']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 5: Pipeline Creation & Health
  // ---------------------------------------------------------------------------
  addContentSlideWithTable(
    'Pipeline Creation & Health',
    '\u2022  Weekly pipeline creation accelerated: $5.0M \u2192 $5.5M \u2192 $6.4M \u2192 $7.2M (vs. $6.3M target)\n' +
    '\u2022  Q3 open pipeline (Sol+): $30.2M \u2192 $32.3M, growing steadily week-over-week\n' +
    '\u2022  IES pipeline mix improved from ~45% of SQOs (Q2) to ~65% (Q3) \u2014 reflecting IES-first focus\n' +
    '\u2022  Next-quarter pipeline surging: $3.0M \u2192 $5.4M \u2192 $7.1M (+32% WoW in latest week)\n' +
    '\u2022  Pipeline value creation increased 19.9% vs. QTD average in the most recent week',
    ['Week', 'Date', 'Qtr Pipeline (Sol+)', 'IES Pipeline', 'Notes'],
    [
      ['Wk 14', '10/31/25', '$32.7M', 'N/A', 'Q1 close, Q2 pipe to build'],
      ['Wk 15', '11/7/25', '$33.2M', '$27.3M', 'IES pipeline +4.6% WoW'],
      ['Wk 17', '11/21/25', '$47.2M', '$39.7M', 'Up 39.6% WoW (Proj Surge)'],
      ['Wk 27', '1/30/26', '$30.1M*', '$24.1M*', 'Next Q; $6.8M created (+42%)'],
      ['Wk 30', '2/21/26', '$31.4M', '$27.9M', '+4% WoW; $5.0M created'],
      ['Wk 31', '2/28/26', '$32.1M', '$28.7M', '$5.5M created (CY high)'],
      ['Wk 32', '3/8/26', '$31.9M', '$28.4M', '$6.4M created - record 13+ wks'],
      ['Wk 33', '3/15/26', '$32.3M', '$28.8M', '$7.2M - 19.9% above QTD avg'],
    ],
    ['$7.2M', '$6.3M', '~65%', '19.9%', '$7.1M']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 6: BDR & Top-of-Funnel
  // ---------------------------------------------------------------------------
  addContentSlide(
    'BDR Performance & Top-of-Funnel',
    '\u2022  BDR SQL volume: 5,789 QTD at 123% of financial target (as of Q2); ' +
    'averaging ~700 SQLs/week with ~35% SQO rate\n\n' +
    '\u2022  BDR-sourced opportunities represent ~34\u201340% of total Solution+ pipeline, ' +
    'with +20% higher IES avg deal size vs. non-BDR opportunities\n\n' +
    '\u2022  Full-Cycle BDR cohort results: 12 IES deals over 8 weeks, $140K incremental, ' +
    'sub-30 day sales cycles \u2014 validating the E2E model as both revenue-generative ' +
    'and a high-velocity AE talent incubator\n\n' +
    '\u2022  BDR-to-AE promotion pipeline producing faster ramp times; some new AEs ' +
    'closing within two weeks of going live\n\n' +
    '\u2022  75% of MTD Solution+ from BDR is IES-focused (up from prior mix), ' +
    'reflecting improved qualification rigor and IES-first alignment\n\n' +
    '\u2022  Record activity: 25K calls and 33K emails in a single week; meeting no-show ' +
    'rate dropped to 13-week low of 9.4%',
    ['123%', '~34\u201340%', '+20%', '12 IES deals', '$140K', 'sub-30 day',
     '75%', '25K calls', '33K emails', '9.4%']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 7: Sales Execution & Ops
  // ---------------------------------------------------------------------------
  addContentSlide(
    'Sales Execution & Operational Improvements',
    '\u2022  Stage velocity improving: average stage durations down 50%+ vs. two weeks prior ' +
    'due to pipeline hygiene and deal advancement\n\n' +
    '\u2022  Gong AI coaching pilot active and expanding \u2014 structured weekly seller-level ' +
    'feedback (2 targeted actions per seller); forecast & deal inspection pilot activated\n\n' +
    '\u2022  Orum dialer launch accelerated to 3/12 to increase dialing capacity and ' +
    'speed-to-connect; warm-transfer model going live with Orum rollout\n\n' +
    '\u2022  Next-Best-Account (NBA) pilot removing territory friction, pairing high-propensity ' +
    'accounts with pre-built sequences for faster outreach\n\n' +
    '\u2022  West region reduced commit age by 77% (90 \u2192 20 days); East implemented daily ' +
    'forecast commitments from managers\n\n' +
    '\u2022  Customer-facing time improved from 20% (Q1) to 24% and trending upward; ' +
    'IES cycle times stabilized at ~30\u201332 days',
    ['50%+', '77%', '90 \u2192 20 days', '20%', '24%', '~30\u201332 days']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 8: Renewals & Retention
  // ---------------------------------------------------------------------------
  addContentSlide(
    'Renewals & Retention',
    '\u2022  NRR trajectory: 89.83% (Q1 close) \u2192 90.79% \u2192 91.79% \u2192 92.84% \u2192 92.89% \u2192 ' +
    '110% (rolling weighted view as of Q2 close)\n\n' +
    '\u2022  Lighthouse customers consistently performing at 94\u201397% NRR, validating ' +
    'proactive outreach model\n\n' +
    '\u2022  Wave 3 renewals completed (12/17); Wave 4: 497 upcoming, ~90 in final stages, ' +
    'proactive outreach complete through early March\n\n' +
    '\u2022  6 Renewal Consultants went live 3/16, expanding capacity and freeing AE bandwidth; ' +
    '6 additional contract workers onboarded 2/2\n\n' +
    '\u2022  Renewals org progressing toward single source of truth forecast model, with ' +
    'manual validation tracking 80\u201390% renewal rates\n\n' +
    '\u2022  Multi-product attach driving retention: 44% attach rate on IES deals vs. 5% non-IES; ' +
    'Payments attach improved 2X since September',
    ['110%', '94\u201397% NRR', '6 Renewal Consultants', '44%', '2X']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 9: Hiring & Org Scaling
  // ---------------------------------------------------------------------------
  addContentSlide(
    'Hiring & Org Scaling',
    '\u2022  2H Acceleration Tranche 1: 24/24 seller roles filled; Tranches 2 & 3 open ' +
    'and in progress\n\n' +
    '\u2022  20 AE / Franchise AE offers accepted across current hiring tranches\n\n' +
    '\u2022  8 new KAMs recently onboarded, with additional backfills underway across regions\n\n' +
    '\u2022  BDR-to-AE promotion pipeline continues producing faster ramp times \u2014 ' +
    'Full-Cycle BDR cohort (AE promotes) set benchmarks: 12 IES deals over 8 weeks\n\n' +
    '\u2022  Key leadership additions: Norman Boyd (Group Manager, ex-NetSuite, started 1/26), ' +
    'David Yasson moved to IES Field Strategist, Megan Moran promoted to GM Channel, ' +
    'Amanda Ellis (Renewals Leader, started 11/24), Julie Lewis (M2 KAM, started 11/17)\n\n' +
    '\u2022  6 contract workers for renewals (started 2/2); data science lead for renewal ' +
    'reporting (started 2/2); 5 BDR hires started in February',
    ['24/24', '20 AE', '8 new KAMs', '12 IES deals']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 10: Key Risks & Blockers
  // ---------------------------------------------------------------------------
  addContentSlide(
    'Key Risks & Blockers',
    '\u2022  Post-sale support drag: Order placement, billing, and customer follow-ups ' +
    'continue reducing seller customer-facing time \u2014 the #1 blocker cited consistently\n\n' +
    '\u2022  Demand generation structural gap: Absence of a consistent marketing demand gen ' +
    'engine delivering IES SQOs contributes to pacing volatility; pipeline mix remains ' +
    '~60/40 vs. 80/20 IES target\n\n' +
    '\u2022  Payments & QBO Advanced competitiveness: HCM functional gaps and pricing ' +
    'misalignment pressure win rates in the 100+ employee segment vs. ADP and Sage\n\n' +
    '\u2022  SFDC system discrepancies: Actual sales performance not reflecting accurately, ' +
    'forcing leaders to toggle between systems; wholesale billing attribution gaps ' +
    'delay revenue recognition\n\n' +
    '\u2022  Pipeline aging risk: 71% of IES opportunities showed aging signals at Q3 start ' +
    'due to Q2 deal pull-forward; actively managed through FLM inspection\n\n' +
    '\u2022  Amazon Connect dialer latency (up to 3-minute delays) impacted seller efficiency ' +
    'in Q2; Orum launch (3/12) is the planned mitigation',
    ['#1 blocker', '~60/40 vs. 80/20', '71%', '3-minute delays']
  );

  // ---------------------------------------------------------------------------
  // SLIDE 11: Tableau Placeholder
  // ---------------------------------------------------------------------------
  var s11 = pres.appendSlide(SlidesApp.PredefinedLayout.BLANK);

  var bar11 = s11.insertShape(SlidesApp.ShapeType.RECTANGLE, 0, 0, W, H * 0.1);
  bar11.getFill().setSolidFill(DARK_BLUE);
  bar11.getBorder().setTransparent();
  bar11.getText().setText('  Unified Funnel: Online Sales & Units Trends');
  bar11.getText().getTextStyle()
    .setFontSize(22).setBold(true).setForegroundColor(WHITE).setFontFamily('Arial');

  var sub11 = s11.insertTextBox(
    'Data Source: Tableau \u2014 SBG Marketing/Sales \u2192 Sales_funnel_v2 \u2192 Unified Funnel',
    W * 0.03, H * 0.11, W * 0.94, H * 0.05
  );
  sub11.getText().getTextStyle()
    .setFontSize(11).setItalic(true).setForegroundColor(MEDIUM_BLUE).setFontFamily('Arial');

  var ph = s11.insertTextBox(
    '[PLACEHOLDER \u2014 Tableau Authentication Required]\n\n' +
    'This slide will display:\n\n' +
    '  1.  Total Online Sales ($) \u2014 trended over time (monthly/weekly)\n' +
    '  2.  Total Units \u2014 trended over time (monthly/weekly)\n\n' +
    'Correlation Analysis:\n' +
    '  \u2022  Compare funnel conversion metrics against pipeline creation trends\n' +
    '     from the exec comms (pipeline creation hit $7.2M in Wk 33)\n' +
    '  \u2022  Overlay IES contract volume trajectory to identify leading/lagging\n' +
    '     indicators between top-of-funnel activity and closed deals\n' +
    '  \u2022  Assess whether Online Sales $ growth rate aligns with the IES mix\n' +
    '     improvement (45% \u2192 65% of SQOs) observed in the exec comms\n\n' +
    'To populate: Authenticate with Tableau MCP, then query the Unified Funnel\n' +
    'datasource for \'Total Online Sales ($)\' and \'Total Units\' aggregated by\n' +
    'TRUNC_MONTH, sorted ASC.',
    W * 0.05, H * 0.18, W * 0.9, H * 0.78
  );
  ph.getText().getTextStyle()
    .setFontSize(12).setForegroundColor(DARK_GRAY).setFontFamily('Arial');

  var phText = ph.getText().asString();
  var headerEnd = phText.indexOf('\n');
  ph.getText().getRange(0, headerEnd).getTextStyle()
    .setBold(true).setFontSize(16).setForegroundColor(MEDIUM_BLUE);
  var caIdx = phText.indexOf('Correlation Analysis:');
  if (caIdx >= 0) {
    ph.getText().getRange(caIdx, caIdx + 'Correlation Analysis:'.length).getTextStyle()
      .setBold(true).setFontSize(13);
  }

  // ---------------------------------------------------------------------------
  // SLIDE 12: Conclusion & Forward Look
  // ---------------------------------------------------------------------------
  addConclusionSlide(
    'Q3 Priorities & Forward Look',
    '1.  Accelerate pipeline conversion through tighter deal inspection\n' +
    '     and stage-based forecasting\n\n' +
    '2.  Activate Win Rooms for strategic six-figure opportunities\n' +
    '     (Diocese deals, large franchise accounts)\n\n' +
    '3.  Deploy demand gen + BDR follow-up against 300+ franchise\n' +
    '     conference leads from IFA\n\n' +
    '4.  Expand Gong insights and intent data signals (Bombora / G2)\n' +
    '     to drive earlier pipeline identification and risk detection\n\n' +
    '5.  Start BDR-to-Seller warm hand-off pilot with Orum integration\n\n' +
    '6.  Create and execute Gap Plan to bridge remaining Q3 targets'
  );

  Logger.log('Done! ' + pres.getSlides().length + ' slides created.');
}
