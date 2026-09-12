
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import arabic_reshaper
from bidi.algorithm import get_display

def farsi(text):
    reshaped = arabic_reshaper.reshape(text)
    return get_display(reshaped)

pdfmetrics.registerFont(TTFont("DejaVu", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"))

doc = SimpleDocTemplate("sql_commands_farsi_fixed.pdf", pagesize=A4, rightMargin=40, leftMargin=40, topMargin=60, bottomMargin=40)

style_rtl = ParagraphStyle(name='RightAlign', fontName="DejaVu", fontSize=12, alignment=TA_RIGHT)
content = []

examples = [
    {
        "title": "SELECT - انتخاب داده‌ها",
        "body": """برای انتخاب داده‌ها از جدول استفاده می‌شود.
مثال:
فرض کنید جدول کاربران (Users) به صورت زیر است:
+----+----------+----------+
| ID | Name     | Age      |
+----+----------+----------+
| 1  | علی      | 25       |
| 2  | زهرا     | 30       |
| 3  | مهدی     | 28       |
+----+----------+----------+

دستور:
SELECT * FROM Users;
نتیجه: تمام ردیف‌های جدول را نشان می‌دهد.

SELECT Name FROM Users WHERE Age > 27;
نتیجه: نام کسانی که سن آنها بیشتر از ۲۷ است."""
    },
    {
        "title": "INSERT - وارد کردن داده",
        "body": """برای افزودن ردیف جدید به جدول.
مثال:
INSERT INTO Users (Name, Age) VALUES ('سارا', 22);

نتیجه: ردیف جدید با نام سارا و سن ۲۲ به جدول اضافه می‌شود."""
    },
    {
        "title": "UPDATE - به‌روزرسانی داده",
        "body": """برای تغییر مقدار موجود در جدول.
مثال:
UPDATE Users SET Age = 26 WHERE Name = 'علی';

نتیجه: سن علی به ۲۶ تغییر می‌یابد."""
    },
    {
        "title": "DELETE - حذف داده",
        "body": """برای حذف داده‌ها از جدول.
مثال:
DELETE FROM Users WHERE Age < 25;

نتیجه: تمام ردیف‌هایی که سن آنها کمتر از ۲۵ است حذف می‌شوند."""
    },
    {
        "title": "JOIN - اتصال جدول‌ها",
        "body": """برای ترکیب داده‌ها از دو یا چند جدول استفاده می‌شود.

فرض کنید دو جدول داریم:
Users:
+----+----------+
| ID | Name     |
+----+----------+
| 1  | علی      |
| 2  | زهرا     |
+----+----------+

Orders:
+----+----------+------------+
| ID | UserID   | Product    |
+----+----------+------------+
| 1  | 1        | کتاب       |
| 2  | 2        | لپ‌تاپ     |
+----+----------+------------+

INNER JOIN:
SELECT Users.Name, Orders.Product FROM Users
INNER JOIN Orders ON Users.ID = Orders.UserID;

LEFT JOIN:
SELECT Users.Name, Orders.Product FROM Users
LEFT JOIN Orders ON Users.ID = Orders.UserID;

RIGHT JOIN:
SELECT Users.Name, Orders.Product FROM Users
RIGHT JOIN Orders ON Users.ID = Orders.UserID;"""
    },
    {
        "title": "دستورات دیگر",
        "body": """CREATE TABLE - ساخت جدول جدید:
CREATE TABLE Products (ID INT, Name VARCHAR(100));

ALTER TABLE - تغییر ساختار جدول:
ALTER TABLE Users ADD Email VARCHAR(100);

DROP TABLE - حذف کامل جدول:
DROP TABLE Orders;

GROUP BY - گروه‌بندی نتایج:
SELECT Age, COUNT(*) FROM Users GROUP BY Age;

ORDER BY - مرتب‌سازی نتایج:
SELECT * FROM Users ORDER BY Age DESC;

LIKE - جستجوی الگو:
SELECT * FROM Users WHERE Name LIKE 'ع%';"""
    }
]

content.append(Paragraph(farsi("دستورات مهم SQL با مثال‌های ساده"), ParagraphStyle(name='Title', fontName="DejaVu", fontSize=16, alignment=TA_RIGHT)))
content.append(Spacer(1, 12))

for item in examples:
    content.append(Paragraph(f"<b>{farsi(item['title'])}</b>", style_rtl))
    content.append(Spacer(1, 6))
    for line in item['body'].split('\n'):
        content.append(Paragraph(farsi(line), style_rtl))
    content.append(Spacer(1, 12))

doc.build(content)
print("✅ فایل sql_commands_farsi_fixed.pdf با موفقیت ساخته شد.")
