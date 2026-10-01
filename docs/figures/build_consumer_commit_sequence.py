"""Build the compact processing/commit sequence used in the methodology."""
import argparse
from pathlib import Path
import reportlab
from reportlab.lib.colors import HexColor
from reportlab.pdfbase.pdfmetrics import registerFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas


def build(destination):
    fonts = Path(reportlab.__file__).parent / 'fonts'
    registerFont(TTFont('Sequence', str(fonts / 'Vera.ttf')))
    registerFont(TTFont('Sequence-Bold', str(fonts / 'VeraBd.ttf')))
    destination.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(destination), pagesize=(720, 170))
    c.setTitle('Processing and commit sequence')
    c.setAuthor('')

    def text(x, y, value, size, color, bold=False, center=False):
        c.setFillColor(HexColor(color))
        c.setFont('Sequence-Bold' if bold else 'Sequence', size)
        (c.drawCentredString if center else c.drawString)(x, y, value)

    def arrow(x1, y, x2):
        c.setStrokeColor(HexColor('#536273'))
        c.setFillColor(HexColor('#536273'))
        c.setLineWidth(1.1)
        c.line(x1, y, x2, y)
        p = c.beginPath()
        p.moveTo(x2, y)
        p.lineTo(x2-4, y+2)
        p.lineTo(x2-4, y-2)
        p.close()
        c.drawPath(p, fill=1, stroke=0)

    text(20, 149, 'Processing and commit sequence', 16, '#203042', True)
    nodes = [
        (['Producer', 'creates', 'a message'], '#F0F3F6', '#536273'),
        (['Consumer', 'receives', 'a batch'], '#E9F2FA', '#23649A'),
        (['Application', 'processes', 'one message'], '#FFF3DE', '#A76914'),
        (['Completion', 'time is', 'recorded'], '#EAF5EE', '#287357'),
        (['Consumer', 'requests an', 'offset commit'], '#F2ECF8', '#70519A'),
        (['Kafka', 'confirms', 'the commit'], '#F2ECF8', '#70519A'),
    ]
    for i, (lines, fill, edge) in enumerate(nodes):
        x = 20+115*i
        c.setFillColor(HexColor(fill))
        c.setStrokeColor(HexColor(edge))
        c.setLineWidth(.8)
        c.roundRect(x, 66, 105, 66, 6, fill=1, stroke=1)
        for j, line in enumerate(lines):
            text(x+52.5, 113-17*j, line, 11.5, edge, True, True)
        if i < 5:
            arrow(x+106, 99, x+114)

    c.setStrokeColor(HexColor('#287357'))
    c.setLineWidth(1)
    c.lines([(72, 59, 72, 51), (72, 51, 418, 51), (418, 51, 418, 59)])
    text(245, 31, 'Completion latency', 12, '#287357', True, True)
    text(590, 49, 'Periodic commits can cover several', 10, '#70519A', center=True)
    text(590, 35, 'completed messages.', 10, '#70519A', center=True)
    text(20, 12, 'Commit acknowledgment is outside the completion-latency interval.', 10, '#536273')
    c.showPage()
    c.save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    build(args.output)
