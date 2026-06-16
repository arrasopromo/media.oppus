const PDFDocument = require('pdfkit');
const fs = require('fs');

const OUT = 'C:\\Users\\PC\\Documents\\meuapp\\Tabela_Precos_Oppus.pdf';
const PURPLE = '#6B46C1';
const PURPLE_DK = '#4c2f91';
const GREYBG = '#f3f0fb';
const TXT = '#1f2937';

const doc = new PDFDocument({ size: 'A4', margin: 40, info: { Title: 'Agência Oppus — Tabela de Preços', Author: 'Agência Oppus' } });
doc.pipe(fs.createWriteStream(OUT));

const PAGE_W = doc.page.width;
const M = 40;
const CONTENT_W = PAGE_W - M * 2;
const BOTTOM = doc.page.height - 50;

function ensure(h) { if (doc.y + h > BOTTOM) { doc.addPage(); doc.y = M; } }

function sectionTitle(txt, emoji) {
  ensure(40);
  doc.moveDown(0.4);
  const y = doc.y;
  doc.save();
  doc.rect(M, y, 4, 18).fill(PURPLE);
  doc.restore();
  doc.fillColor(PURPLE_DK).font('Helvetica-Bold').fontSize(14).text((emoji ? emoji + '  ' : '') + txt, M + 12, y + 1);
  doc.moveDown(0.5);
  doc.fillColor(TXT);
}

function drawTable(headers, rows, colWidths) {
  const rowH = 20;
  const startX = M;
  // header
  ensure(rowH + 4);
  let y = doc.y;
  doc.save();
  doc.rect(startX, y, CONTENT_W, rowH).fill(PURPLE);
  doc.fillColor('#ffffff').font('Helvetica-Bold').fontSize(9.5);
  let x = startX;
  headers.forEach((h, i) => {
    const align = i === 0 ? 'left' : 'right';
    doc.text(h, x + 6, y + 6, { width: colWidths[i] - 12, align });
    x += colWidths[i];
  });
  doc.restore();
  y += rowH;
  // rows
  doc.font('Helvetica').fontSize(9.5);
  rows.forEach((row, ri) => {
    if (y + rowH > BOTTOM) { doc.addPage(); y = M;
      // repeat header
      doc.save(); doc.rect(startX, y, CONTENT_W, rowH).fill(PURPLE); doc.fillColor('#fff').font('Helvetica-Bold').fontSize(9.5);
      let xx = startX; headers.forEach((h, i) => { doc.text(h, xx + 6, y + 6, { width: colWidths[i] - 12, align: i === 0 ? 'left' : 'right' }); xx += colWidths[i]; }); doc.restore();
      y += rowH; doc.font('Helvetica').fontSize(9.5);
    }
    if (ri % 2 === 0) { doc.save(); doc.rect(startX, y, CONTENT_W, rowH).fill(GREYBG); doc.restore(); }
    let cx = startX;
    row.forEach((cell, i) => {
      const align = i === 0 ? 'left' : 'right';
      doc.fillColor(i === 0 ? PURPLE_DK : TXT).font(i === 0 ? 'Helvetica-Bold' : 'Helvetica');
      doc.text(String(cell), cx + 6, y + 6, { width: colWidths[i] - 12, align });
      cx += colWidths[i];
    });
    y += rowH;
  });
  // border
  doc.save(); doc.lineWidth(0.5).strokeColor('#d1c7ee').rect(startX, doc.y, CONTENT_W, y - doc.y).stroke(); doc.restore();
  doc.y = y;
  doc.fillColor(TXT);
}

function note(txt) {
  ensure(16);
  doc.font('Helvetica').fontSize(8.5).fillColor('#6b7280').text(txt, M, doc.y + 2, { width: CONTENT_W });
  doc.fillColor(TXT);
}

// ===== HEADER =====
doc.rect(0, 0, PAGE_W, 70).fill(PURPLE);
doc.fillColor('#ffffff').font('Helvetica-Bold').fontSize(22).text('Agência Oppus', M, 18);
doc.font('Helvetica').fontSize(11).fillColor('#e9e2fb').text('Tabela de Preços e Serviços', M, 46);
const hoje = new Date().toLocaleDateString('pt-BR');
doc.font('Helvetica').fontSize(9).fillColor('#e9e2fb').text('Atualizado em ' + hoje + '  ·  Pagamento via PIX', M, 46, { width: CONTENT_W, align: 'right' });
doc.y = 86; doc.fillColor(TXT);

const col4 = [CONTENT_W * 0.22, CONTENT_W * 0.26, CONTENT_W * 0.26, CONTENT_W * 0.26];

// ===== SEGUIDORES =====
sectionTitle('SEGUIDORES', '');
drawTable(['Quantidade', 'Mistos', 'Brasileiros', 'Reais (orgânicos)'], [
  ['100', 'R$ 3,00 *', '—', '—'],
  ['150', 'R$ 7,90', 'R$ 12,90', 'R$ 39,90'],
  ['300', 'R$ 12,90', 'R$ 24,90', 'R$ 49,90'],
  ['500', 'R$ 16,90', 'R$ 39,90', 'R$ 69,90'],
  ['700', 'R$ 22,90', 'R$ 49,90', 'R$ 89,90'],
  ['1.000', 'R$ 29,90', 'R$ 79,90', 'R$ 129,90'],
  ['2.000', 'R$ 49,90', 'R$ 129,90', 'R$ 199,90'],
  ['3.000', 'R$ 79,90', 'R$ 179,90', 'R$ 249,90'],
  ['4.000', 'R$ 99,90', 'R$ 249,90', 'R$ 329,90'],
  ['5.000', 'R$ 129,90', 'R$ 279,90', 'R$ 499,90'],
  ['7.500', 'R$ 169,90', 'R$ 399,90', 'R$ 599,90'],
  ['10.000', 'R$ 229,90', 'R$ 499,90', 'R$ 899,90'],
  ['15.000', 'R$ 329,90', 'R$ 799,90', 'R$ 1.299,90'],
], col4);
note('* Pacote de 100 (R$ 3,00) é exclusivo do upsell — não aparece no catálogo normal.');
note('Provedor: Mistos → fama24h #663  ·  Brasileiros → fama24h #23  ·  Reais → fornecedor_social #312');

// ===== CURTIDAS =====
sectionTitle('CURTIDAS', '');
drawTable(['Quantidade', 'Mistas', 'Brasileiras', 'Reais'], [
  ['150', 'R$ 4,90', 'R$ 4,90', 'R$ 16,90'],
  ['300', 'R$ 7,90', 'R$ 9,90', 'R$ 28,90'],
  ['500', 'R$ 9,90', 'R$ 14,90', 'R$ 49,90'],
  ['700', 'R$ 14,90', 'R$ 29,90', '—'],
  ['1.000', 'R$ 19,90', 'R$ 39,90', 'R$ 69,90'],
  ['2.000', 'R$ 24,90', 'R$ 49,90', 'R$ 104,90'],
  ['3.000', 'R$ 29,90', 'R$ 59,90', 'R$ 139,90'],
  ['4.000', 'R$ 34,90', 'R$ 69,90', 'R$ 174,90'],
  ['5.000', 'R$ 39,90', 'R$ 79,90', 'R$ 224,90'],
  ['7.500', 'R$ 49,90', 'R$ 109,90', 'R$ 279,90'],
  ['10.000', 'R$ 69,90', 'R$ 139,90', 'R$ 349,90'],
  ['15.000', 'R$ 89,90', 'R$ 199,90', 'R$ 449,90'],
], col4);
note('Curtidas Reais começam em 150 (pacote de 100 removido).');
note('Provedor: Mistas → fama24h #671  ·  Brasileiras → fama24h #679  ·  Reais → topfama #233 (fallback fornecedor_social #194)');

// ===== VISUALIZACOES =====
sectionTitle('VISUALIZAÇÕES (Reels)', '');
drawTable(['Quantidade', 'Preço'], [
  ['1.000', 'R$ 4,90'], ['2.500', 'R$ 9,90'], ['5.000', 'R$ 14,90'], ['10.000', 'R$ 19,90'],
  ['25.000', 'R$ 24,90'], ['50.000', 'R$ 34,90'], ['100.000', 'R$ 49,90'], ['150.000', 'R$ 59,90'],
  ['200.000', 'R$ 69,90'], ['250.000', 'R$ 89,90'], ['500.000', 'R$ 109,90'], ['1.000.000', 'R$ 159,90'],
], [CONTENT_W * 0.5, CONTENT_W * 0.5]);
note('Provedor: fama24h #250');

// ===== ORDER BUMPS =====
sectionTitle('ORDER-BUMPS (adicionais no checkout)', '');
doc.font('Helvetica-Bold').fontSize(10).fillColor(TXT).text('Curtidas (bump) — preço varia pelo tipo do pedido base:', M, doc.y + 2);
doc.moveDown(0.3);
drawTable(['Quantidade', 'Mistas', 'Brasileiras', 'Reais'], [
  ['150', 'R$ 4,90', 'R$ 5,90', 'R$ 16,90'],
  ['300', 'R$ 9,90', 'R$ 9,90', 'R$ 28,90'],
  ['500', 'R$ 14,90', 'R$ 14,90', 'R$ 49,90'],
  ['700', 'R$ 19,90', 'R$ 29,90', '—'],
  ['1.000', 'R$ 24,90', 'R$ 39,90', 'R$ 69,90'],
  ['2.000', 'R$ 34,90', 'R$ 49,90', 'R$ 104,90'],
  ['3.000', 'R$ 49,90', 'R$ 59,90', 'R$ 139,90'],
  ['4.000', 'R$ 59,90', 'R$ 69,90', 'R$ 174,90'],
  ['5.000', 'R$ 69,90', 'R$ 79,90', 'R$ 224,90'],
  ['7.500', 'R$ 89,90', 'R$ 109,90', 'R$ 279,90'],
  ['10.000', 'R$ 109,90', 'R$ 139,90', 'R$ 349,90'],
  ['15.000', 'R$ 159,90', 'R$ 199,90', 'R$ 449,90'],
], col4);
note('Bump de Visualizações → usa a tabela de Visualizações.   Bump de Comentários → WorldSMM #90.');
note('Provedor do bump de curtidas: Mistas/padrão → fama24h #671  ·  Brasileiras → fama24h #679  ·  Reais → topfama #233');

// ===== REPOSICAO =====
sectionTitle('REPOSIÇÃO (Refil)', '');
doc.font('Helvetica').fontSize(9.5).fillColor(TXT).text(
  '• Disponível para seguidores Mistos e Brasileiros (via fama24h, action=refill).\n' +
  '• Aguardar 24h entre uma reposição e outra.\n' +
  '• Não é possível solicitar no mesmo dia da compra (a reposição é para repor a queda ao longo do tempo).\n' +
  '• Perfil precisa estar público e não pode ter trocado o @.',
  M, doc.y + 2, { width: CONTENT_W, lineGap: 2 });

doc.end();
console.log('PDF gerado:', OUT);
