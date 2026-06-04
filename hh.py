import arabic_reshaper
from bidi.algorithm import get_display
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.enums import TA_CENTER, TA_RIGHT

font_path = "/home/claude/Amiri-Regular.ttf"
font_bold_path = "/home/claude/Amiri-Bold.ttf"
pdfmetrics.registerFont(TTFont('Amiri', font_path))
pdfmetrics.registerFont(TTFont('Amiri-Bold', font_bold_path))

def ar(text):
    reshaped = arabic_reshaper.reshape(text)
    return get_display(reshaped)

doc = SimpleDocTemplate(
    "/mnt/user-data/outputs/resume_cours2_valeurs_objectifs.pdf",
    pagesize=A4,
    rightMargin=1.5*cm, leftMargin=1.5*cm,
    topMargin=1.5*cm, bottomMargin=1.5*cm,
)

title_style = ParagraphStyle('title', fontName='Amiri-Bold', fontSize=20, alignment=TA_CENTER,
    textColor=colors.HexColor('#1a237e'), spaceAfter=8, leading=28)
main_style = ParagraphStyle('main', fontName='Amiri-Bold', fontSize=15, alignment=TA_RIGHT,
    textColor=colors.HexColor('#b71c1c'), spaceAfter=4, spaceBefore=12, leading=22)
sub_style = ParagraphStyle('sub', fontName='Amiri-Bold', fontSize=13, alignment=TA_RIGHT,
    textColor=colors.HexColor('#1565c0'), spaceAfter=4, spaceBefore=8, leading=20)
body_style = ParagraphStyle('body', fontName='Amiri', fontSize=11, alignment=TA_RIGHT,
    spaceAfter=4, leading=18, textColor=colors.HexColor('#212121'))
bullet_style = ParagraphStyle('bullet', fontName='Amiri', fontSize=11, alignment=TA_RIGHT,
    spaceAfter=3, leading=18, rightIndent=20, textColor=colors.HexColor('#212121'))
box_title_style = ParagraphStyle('box_title', fontName='Amiri-Bold', fontSize=12, alignment=TA_RIGHT,
    textColor=colors.HexColor('#e65100'), leading=18)

story = []

# ── TITLE ──
story.append(Paragraph(ar("ملخص: القيم والأهداف — نماذج SWOT ونافذة جوهاري"), title_style))
story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1a237e'), spaceAfter=10))

# ═══════════════════════════════════════════════════
# PART 1 — VALUES & GOALS
# ═══════════════════════════════════════════════════
story.append(Paragraph(ar("المحور: القيم والأهداف"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

# Intro
story.append(Paragraph(ar("تقديم: في أهمية القيم"), sub_style))
story.append(Paragraph(ar("تتصدر القيم مكانة رفيعة في المجتمعات وتحتل مراتب راقية في الإطار المرجعي لكل فرد وأسرة ومجتمع. وفي الدين الإسلامي خاصة كان النبي ﷺ يؤكد على القيم قبل الإسلام وبعده، إذ قال: «إنما بُعثت لأُتمم مكارم الأخلاق». وبما أن هذا العمل مندرج ضمن المنظور الفردي للقيم، فسيكون التركيز فيه على القيم على وجه العموم."), body_style))

# 1. Values definition
story.append(Paragraph(ar("أولًا: التمييز بين القيمة والهدف"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

story.append(Paragraph(ar("1– تعريف القيم"), sub_style))
story.append(Paragraph(ar("القيمة: جملة المعتقدات التي تحملها الفرد نحو الأشياء والمعاني وأوجه النشاط المختلفة، والتي تعمل على توجيه رغباته واتجاهاته نحوها وتحدد له السلوك المقبول أو المرفوض، والصواب أو الخطأ، وكل هذا بنسبية ظاهرة لا سبيل إلى نكرانها."), body_style))
story.append(Spacer(1, 4))
story.append(Paragraph(ar("التعريف الإجرائي:"), box_title_style))
story.append(Paragraph(ar("القيم هي مجموعة المبادئ الراقية والمعايير السامية والمثل العليا التي تتحكم في أغلب السلوك الإنساني وتوجهه نحو التصرف المناسب."), body_style))

# 2. Goal definition
story.append(Spacer(1, 6))
story.append(Paragraph(ar("2– تعريف الهدف"), sub_style))
story.append(Paragraph(ar("الهدف هو غاية محددة ومباشرة يسعى الفرد أو الجماعة إلى تحقيقها خلال زمن معين وبإمكانات محدودة."), body_style))
story.append(Spacer(1, 4))
goal_chars = [
    "الأهداف تكون عملية.",
    "الأهداف قابلة للقياس والمراجعة.",
    "الأهداف تمثل خطوات تطبيقية لتحقيق معنى أو قيمة عليا.",
    "مثال: الحصول على شهادة جامعية، إنجاز مشروع، تعلم لغة جديدة.",
]
for item in goal_chars:
    story.append(Paragraph(ar(f"  ✓ {item}"), bullet_style))

# 3. Differences
story.append(Spacer(1, 6))
story.append(Paragraph(ar("3– الفرق بين القيم والأهداف"), sub_style))
diff_data = [
    [Paragraph(ar("الأهداف"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("القيم"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white))],
    [Paragraph(ar("هي الوجهة التي يجب الوصول إليها."), body_style),
     Paragraph(ar("تعطينا إحساسًا بالاتجاه."), body_style)],
    [Paragraph(ar("يمكن تحقيق الأهداف وتنتهي."), body_style),
     Paragraph(ar("القيم دائمة لا تنتهي."), body_style)],
    [Paragraph(ar("الأهداف نتيجة للعالم الخارجي — مرنة."), body_style),
     Paragraph(ar("القيم تأتي من داخلنا — غير مرنة بطبيعتها."), body_style)],
    [Paragraph(ar("الأهداف تتعلق بنقطة معينة في المستقبل."), body_style),
     Paragraph(ar("القيم مستمرة وموجِّهة دائمًا."), body_style)],
]
t = Table(diff_data, colWidths=[8.5*cm, 8.5*cm])
t.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1565c0')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#90caf9')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#e3f2fd'), colors.white]),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(t)
story.append(Spacer(1, 4))
story.append(Paragraph(ar("لذا، مع أن كليهما مفيد لفهم أنفسنا والسير في اتجاه قيم، إلا أن القيم والأهداف تختلفان في جوانب جوهرية."), body_style))

# ═══════════════════════════════════════════════════
# PART 2 — PERSONAL VALUES
# ═══════════════════════════════════════════════════
story.append(Paragraph(ar("ثانيًا: تقييم القيم الشخصية"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

story.append(Paragraph(ar("القيم الشخصية هي القيم التي تتعلق بشخصية الفرد من جوانبها النفسية والعقلية، حيث يتبناها الفرد ويستخدمها في ذاته وتشكّل نسقه المعرفي الذي يوجه تصرفاته وسلوكه، وتغدو لديه معتقدات ثابتة نسبيًا أو قناعات يتصرف بموجبها."), body_style))

story.append(Spacer(1, 4))
# Two types
data2 = [
    [Paragraph(ar("2– قيم فكرية عقلية"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("1– قيم ذاتية"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white))],
    [Paragraph(ar("هي مجموعة المعايير والأحكام التي تعمل كموجهات وضوابط للتفكير العملي، وتشكّل اتجاهات إيجابية نحو العلم والتعلم وتنمية الشخصية في جانبها العقلي المعرفي."), body_style),
     Paragraph(ar("هي معتقدات ثابتة نسبيًا تمثل للفرد أحكامًا معيارية أو وسائل أو غايات يسعى لتحقيقها."), body_style)],
]
t2 = Table(data2, colWidths=[8.5*cm, 8.5*cm])
t2.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#b71c1c')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#ef9a9a')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#ffebee'), colors.white]),
    ('TOPPADDING', (0,0), (-1,-1), 6),
    ('BOTTOMPADDING', (0,0), (-1,-1), 6),
]))
story.append(t2)

story.append(Spacer(1, 6))
story.append(Paragraph(ar("مجالات القيم الشخصية:"), box_title_style))
domains = ["القيم الدينية.", "القيم الاجتماعية.", "القيم الاقتصادية.", "القيم السياسية.", "القيم النظرية.", "القيم الجمالية."]
for i, d in enumerate(domains, 1):
    story.append(Paragraph(ar(f"  {i}– {d}"), bullet_style))

# ═══════════════════════════════════════════════════
# PART 3 — SMART GOALS
# ═══════════════════════════════════════════════════
story.append(Paragraph(ar("ثالثًا: تحديد الأهداف — نظام SMART"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

story.append(Paragraph(ar("يُصبح تحديد الأهداف وتحقيقها عنصرًا أساسيًا للنجاح في الحياة المهنية والشخصية على حد سواء. لكن يبدو وضع الأهداف الذكية وتنفيذها بشكل منظم ومدروس أمرًا مهمًا. لهذا تبرز أهمية الأهداف الذكية أو ما يُعرف بنظام SMART."), body_style))

story.append(Spacer(1, 4))
story.append(Paragraph(ar("الأهداف الذكية SMART:"), box_title_style))
story.append(Paragraph(ar("هي إطار عمل يستخدمه الأفراد والمؤسسات لتحديد وتحقيق الأهداف بفعالية ودقة. يرمز مصطلح SMART إلى خمسة معايير يجب أن تتوافر في الأهداف لتكون فعّالة:"), body_style))

smart_data = [
    [Paragraph(ar("المعيار"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("الرمز"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("المعنى"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white))],
    [Paragraph(ar("محددة"), body_style), Paragraph("Specific", ParagraphStyle('en', fontName='Helvetica-Bold', fontSize=10, alignment=TA_CENTER)),
     Paragraph(ar("يجيب الهدف المحدد على الأسئلة الخمسة الأساسية: ماذا؟ من؟ أين؟ لماذا؟ وكيف؟"), body_style)],
    [Paragraph(ar("قابلة للقياس"), body_style), Paragraph("Measurable", ParagraphStyle('en', fontName='Helvetica-Bold', fontSize=10, alignment=TA_CENTER)),
     Paragraph(ar("تحتاج الأهداف إلى أن تكون قابلة للقياس لتتمكن من متابعة مدى نجاحك في تحقيقها من خلال تحديد معايير أو مؤشرات أداء."), body_style)],
    [Paragraph(ar("قابلة للتحقيق"), body_style), Paragraph("Achievable", ParagraphStyle('en', fontName='Helvetica-Bold', fontSize=10, alignment=TA_CENTER)),
     Paragraph(ar("يجب أن تكون الأهداف واقعية وقابلة للتحقيق بالنظر إلى مهاراتك وموارك. الأهداف الطموحة جيدة، ولكن يجب أن تكون ممكنة."), body_style)],
    [Paragraph(ar("متعلقة"), body_style), Paragraph("Relevant", ParagraphStyle('en', fontName='Helvetica-Bold', fontSize=10, alignment=TA_CENTER)),
     Paragraph(ar("يجب أن تكون أهدافك ذات صلة بتطلعاتك وقيمك وأهدافك الاستراتيجية العامة."), body_style)],
    [Paragraph(ar("محددة زمنيًا"), body_style), Paragraph("Time-bound", ParagraphStyle('en', fontName='Helvetica-Bold', fontSize=10, alignment=TA_CENTER)),
     Paragraph(ar("يجب أن تحدد إطارًا زمنيًا واضحًا لتحقيق كل هدف. الإطار الزمني يساعد في خلق شعور بالإلحاح والتركيز."), body_style)],
]
t3 = Table(smart_data, colWidths=[3.5*cm, 4*cm, 9.5*cm])
t3.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#2e7d32')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#a5d6a7')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#e8f5e9'), colors.white]),
    ('TOPPADDING', (0,0), (-1,-1), 5),
    ('BOTTOMPADDING', (0,0), (-1,-1), 5),
]))
story.append(t3)

story.append(Spacer(1, 6))
story.append(Paragraph(ar("خطوات وضع أهداف ذكية SMART:"), box_title_style))
steps = [
    "ابدأ بتحديد أهدافك الكبيرة.",
    "اسأل نفسك أسئلة SMART.",
    "حدد مؤشرات قياسية.",
    "ضع خطة زمنية.",
    "خصص الموارد اللازمة.",
]
for i, step in enumerate(steps, 1):
    story.append(Paragraph(ar(f"  {i}– {step}"), bullet_style))

# ═══════════════════════════════════════════════════
# PART 4 — SWOT
# ═══════════════════════════════════════════════════
story.append(Paragraph(ar("المحور: التقييم الشخصي والمهني — نماذج التحليل"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

story.append(Paragraph(ar("تقديم:"), box_title_style))
story.append(Paragraph(ar("يُعد التقييم الشخصي والمهني مدخلًا أساسيًا لفهم الذات وتطويرها، إذ يهدف إلى تشخيص القدرات والميول والقيم نحو تحقيق الكفاءة والتميّز في الحياة العلمية والعملية. ويستند هذا التقييم إلى الوعي الذاتي كأساس للنمو المعرفي والسلوكي."), body_style))

story.append(Paragraph(ar("أولًا: نموذج SWOT"), sub_style))

story.append(Paragraph(ar("1– التعريف"), box_title_style))
story.append(Paragraph(ar("نموذج SWOT: هو أداة تحليل استراتيجي تُستخدم في الإدارة والتخطيط لتشخيص وضع مؤسسة أو مشروع أو حتى فرد، وذلك عبر تحديد نقاط القوة (Strengths) ونقاط الضعف (Weaknesses)، ثم الفرص (Opportunities) والتهديدات (Threats). يُعتبر هذا النموذج أحد أشهر أدوات التحليل الرباعي في الدراسات الاستراتيجية والإدارية."), body_style))

story.append(Spacer(1, 4))
story.append(Paragraph(ar("2– النشأة"), box_title_style))
story.append(Paragraph(ar("ظهر النموذج في ستينيات القرن العشرين على يد مجموعة من الباحثين في جامعة ستانفورد بالولايات المتحدة، ضمن مشروع بحثي امتد من 1960 إلى 1970 تحت إشراف ألبرت هامفري (Albert Humphrey). كان الهدف حينها تطوير أداة تساعد الشركات الكبرى على صياغة خطط استراتيجية طويلة المدى."), body_style))

story.append(Spacer(1, 6))
story.append(Paragraph(ar("3– أبعاد النموذج:"), box_title_style))

swot_data = [
    [Paragraph(ar("البُعد الخارجي"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("البُعد الداخلي"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white))],
    [Paragraph(ar("الفرص (O): الظروف الإيجابية في البيئة الخارجية التي يمكن استغلالها (مثل: نمو السوق، الدعم الحكومي، التطورات التكنولوجية).\n\nالتهديدات (T): المخاطر الخارجية التي قد تعرقل الأهداف (مثل: المنافسة الشرسة، التغيرات الاقتصادية، القوانين المُقيِّدة)."), body_style),
     Paragraph(ar("نقاط القوة (S): القدرات والإمكانات التي تمنح المنظمة ميزة تنافسية (مثل: الموارد المالية، الكفاءات البشرية، التكنولوجيا المتقدمة).\n\ننقاط الضعف (W): العوائق الداخلية التي تحد من الأداء (مثل: ضعف الهيكلة، قلة الخبرة، محدودية الموارد)."), body_style)],
]
t4 = Table(swot_data, colWidths=[8.5*cm, 8.5*cm])
t4.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#4a148c')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#ce93d8')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#f3e5f5'), colors.white]),
    ('TOPPADDING', (0,0), (-1,-1), 6),
    ('BOTTOMPADDING', (0,0), (-1,-1), 6),
]))
story.append(t4)

story.append(Spacer(1, 6))
story.append(Paragraph(ar("5– خطوات توظيف SWOT في تطوير الذات:"), box_title_style))

swot_apply = [
    [Paragraph(ar("تحليل البيئة الخارجية"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("تشخيص الذات داخليًا"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white))],
    [Paragraph(ar("الفرص (O):\nمثل: برامج تدريبية متاحة، شبكات اجتماعية داعمة، فرص عمل، مجال مهني معين.\n+ تمثل مساحات للنمو والاستثمار.\n\nالتهديدات (T):\nمثل: المنافسة البشرية في سوق العمل، التغيرات الاقتصادية، الضغوط الاجتماعية.\n+ الوعي بها يحفز الفرد على تبني خطط وقائية."), body_style),
     Paragraph(ar("نقاط القوة (S):\nمثل: مهارات التواصل، الخبرات المهنية، الذكاء العاطفي، الانضباط.\n+ تساعد في تعزيز الثقة بالذات والانطلاق من مكامن التميز.\n\ننقاط الضعف (W):\nمثل: ضعف إدارة الوقت، التردد في اتخاذ القرارات، قلة مهارات تقنية.\n+ الاعتراف بها يمثل أول خطوة في إصلاح الذات وتوجيه التدريب المناسب."), body_style)],
]
t5 = Table(swot_apply, colWidths=[8.5*cm, 8.5*cm])
t5.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#2e7d32')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#a5d6a7')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#e8f5e9'), colors.white]),
    ('TOPPADDING', (0,0), (-1,-1), 6),
    ('BOTTOMPADDING', (0,0), (-1,-1), 6),
]))
story.append(t5)

# ═══════════════════════════════════════════════════
# PART 5 — JOHARI WINDOW
# ═══════════════════════════════════════════════════
story.append(Paragraph(ar("ثانيًا: نافذة جوهاري — Johari Window"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

story.append(Paragraph(ar("1– التعريف"), box_title_style))
story.append(Paragraph(ar("نافذة جوهاري هي أداة سيكولوجية للتواصل وفهم الذات والآخرين، طورها عالما النفس جوزيف لوفت (Joseph Luft) وهارينغتون إنغهام (Harrington Ingham) سنة 1955. تعتمد الفكرة على أن الذات الإنسانية ليست كتلة شفافة، بل تتنوع بين ما هو معلوم للفرد، وما هو معلوم للآخرين، وما هو خفي عن الجميع."), body_style))

story.append(Spacer(1, 4))
story.append(Paragraph(ar("2– الأهمية"), box_title_style))
story.append(Paragraph(ar("تُعد نافذة جوهاري من الأدوات السيكولوجية الأكثر استعمالًا في تنمية الوعي الذاتي وتحسين صورة الذات. فهي تساعد الفرد على استكشاف الذات وتحسين التواصل بين الأفراد والجماعات من خلال علاقة ثلاثية: ما يعرفه عن نفسه، ما يعرفه الآخرون عنه، وما يظل مجهولًا للطرفين."), body_style))

story.append(Spacer(1, 6))
story.append(Paragraph(ar("3– المربعات الأربعة في تطوير الذات:"), box_title_style))

johari_data = [
    [Paragraph(ar("المجال الأعمى"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white, leading=16)),
     Paragraph(ar("المجال المفتوح"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white, leading=16))],
    [Paragraph(ar("• يمثل ما يجهله الفرد عن نفسه بينما يراه الآخرون.\n• تقليصه يعني زيادة الوعي الذاتي.\n• وسيلة التقليص: قبول النقد البنّاء والاستماع لملاحظات الآخرين بصدق."), body_style),
     Paragraph(ar("• يمثل ما يعرفه الفرد عن نفسه ويعرفه الآخرون.\n• كلما توسّع هذا المجال زاد وضوح شخصية الفرد وارتفعت ثقته بنفسه.\n• وسيلة التوسيع: المصارحة، تعزيز التواصل وتقبّل التغذية الراجعة."), body_style)],
    [Paragraph(ar("المجال المجهول"), ParagraphStyle('th2', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white, leading=16)),
     Paragraph(ar("المجال الخفي"), ParagraphStyle('th2', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white, leading=16))],
    [Paragraph(ar("• يمثل القدرات أو المشاعر غير المكتشفة.\n• استكشافه يفتح إمكانات جديدة للفرد.\n• وسيلة الاكتشاف: خوض تجارب جديدة، مواجهة التحديات، التدريب الذاتي."), body_style),
     Paragraph(ar("• يمثل ما يخفيه الفرد عن الآخرين رغم معرفته به.\n• الإفراط في الاتساع يؤدي إلى عزلة داخلية وضعف الثقة بالآخرين.\n• وسيلة التقليص: الإفصاح التدريجي عن المشاعر والطموحات بما يعزز الدعم المتبادل."), body_style)],
]
t6 = Table(johari_data, colWidths=[8.5*cm, 8.5*cm])
t6.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (0,0), colors.HexColor('#e65100')),
    ('BACKGROUND', (1,0), (1,0), colors.HexColor('#1565c0')),
    ('BACKGROUND', (0,2), (0,2), colors.HexColor('#37474f')),
    ('BACKGROUND', (1,2), (1,2), colors.HexColor('#4a148c')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#bdbdbd')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('BACKGROUND', (0,1), (0,1), colors.HexColor('#fff3e0')),
    ('BACKGROUND', (1,1), (1,1), colors.HexColor('#e3f2fd')),
    ('BACKGROUND', (0,3), (0,3), colors.HexColor('#eceff1')),
    ('BACKGROUND', (1,3), (1,3), colors.HexColor('#f3e5f5')),
    ('TOPPADDING', (0,0), (-1,-1), 6),
    ('BOTTOMPADDING', (0,0), (-1,-1), 6),
]))
story.append(t6)

# Stress management (already in course 2 images)
story.append(Spacer(1, 8))
story.append(Paragraph(ar("كيف تتغلب على الضغط؟"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))
story.append(Paragraph(ar("الضغط هو جزء لا يتجزأ من حياتنا، ومن بين أساليب التغلب عليه:"), body_style))
stress_items = [
    ("التخطيط", "يساعد التخطيط على تحديد الأولويات والأهداف والبرامج لإنجاز المهام بشكل فعّال وكفء."),
    ("التفكير", "يساعد على تحليل الموقف بشكل منطقي وواقعي وإيجاد حلول مبدعة ومناسبة، كما يساعد على تقبّل الأخطاء والفشل كجزء من عملية التعلم."),
    ("التواصل", "يساعد على التعبير عن المشاعر والآراء بشكل صريح وودود، والاستماع إلى الآخرين لكل تفهم."),
    ("الاسترخاء", "يُساعد على قدرة الجسم والعقل والروح، وتخفيف التوتر والقلق. من أساليب الاسترخاء: التنفس العميق، والتأمل، والصلاة، والرياضة."),
]
for term, defn in stress_items:
    story.append(Paragraph(ar(f"  ◆ {term}: {defn}"), bullet_style))

# Transactional analysis continuation (child ego states)
story.append(Spacer(1, 8))
story.append(Paragraph(ar("تكملة: حالة الأنا الطفل (نظرية التحليل التبادلي)"), main_style))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#b71c1c'), spaceAfter=6))

child_data = [
    [Paragraph(ar("حالة الأنا الطفل المتكيف"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white)),
     Paragraph(ar("حالة الأنا الطفل الحر"), ParagraphStyle('th', fontName='Amiri-Bold', fontSize=11, alignment=TA_CENTER, textColor=colors.white))],
    [Paragraph(ar("الطفل المتكيف هو الجزء من الشخصية الذي يتطور من الرسائل الأبوية التي يتعلمها الفرد حين يكبر. فهو حالة الأنا التي في الفرد يعتمد على قدراته الخاصة ويحاول التكملة علمه بقبول تأثير الآخرين."), body_style),
     Paragraph(ar("حالة تعنى بالحاجات الجسدية للشخص، بحيث يتصف الشخص في هذه الحالة بالعفوية، ويكون نشيطًا ومبدعًا، ويواجه المواقف بطريقة مباشرة وفورية، غير أنه يميل إلى الجانب غير المتعلم من الشخصية."), body_style)],
]
tc = Table(child_data, colWidths=[8.5*cm, 8.5*cm])
tc.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#00695c')),
    ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#80cbc4')),
    ('ALIGN', (0,0), (-1,-1), 'RIGHT'),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#e0f2f1'), colors.white]),
    ('TOPPADDING', (0,0), (-1,-1), 6),
    ('BOTTOMPADDING', (0,0), (-1,-1), 6),
]))
story.append(tc)
story.append(Spacer(1, 4))
story.append(Paragraph(ar("ملاحظة: هذه هي المجموعة الكاملة من حالات الأنا المتوفرة في الفرد، حيث يتفاعل الشخص حالة الأنا التي سيستخدمها في التبادل (أي كل أنا يرى أي حالة أنا بأسمه). وعليه فإن التفاعل بين شخصين يعني وجود ست حالات أنا، الثلاثة لكل فرد."), body_style))

# Footer
story.append(Spacer(1, 12))
story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1a237e'), spaceAfter=4))
story.append(Paragraph(ar("ملخص المحاضرات — القيم، الأهداف، SWOT، نافذة جوهاري"), ParagraphStyle('footer', fontName='Amiri', fontSize=9, alignment=TA_CENTER, textColor=colors.grey)))

doc.build(story)
print("PDF 2 created successfully!")