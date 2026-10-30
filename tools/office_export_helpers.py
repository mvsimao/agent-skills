"""
Office OpenXML Export Helpers (Pure Standard Library: zipfile + xml.etree.ElementTree).

Enables native generation of valid Microsoft Excel (.xlsx) and PowerPoint (.pptx)
files without requiring third-party libraries (openpyxl or python-pptx).
"""

from __future__ import annotations

import io
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Any, Optional


def create_minimal_xlsx(
    sheets_data: Dict[str, List[List[Any]]],
    output_path: str
) -> str:
    """
    Creates a valid Microsoft Excel workbook (.xlsx) with multiple sheets using standard library.
    sheets_data: {"Sheet1": [["Header1", "Header2"], ["Val1", 123]]}
    """
    out_p = Path(output_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # [Content_Types].xml
        content_types = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
            '  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
            '  <Default Extension="xml" ContentType="application/xml"/>',
            '  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        ]
        sheet_names = list(sheets_data.keys())
        for idx in range(1, len(sheet_names) + 1):
            content_types.append(
                f'  <Override PartName="/xl/worksheets/sheet{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )
        content_types.append('</Types>')
        zf.writestr("[Content_Types].xml", "\n".join(content_types))

        # _rels/.rels
        rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
            '  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>\n'
            '</Relationships>'
        )
        zf.writestr("_rels/.rels", rels)

        # xl/workbook.xml
        wb_lines = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
            '  <sheets>'
        ]
        wb_rels_lines = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        ]
        for idx, sname in enumerate(sheet_names, start=1):
            wb_lines.append(f'    <sheet name="{sname}" sheetId="{idx}" r:id="rId{idx}"/>')
            wb_rels_lines.append(
                f'  <Relationship Id="rId{idx}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{idx}.xml"/>'
            )
        wb_lines.append('  </sheets>')
        wb_lines.append('</workbook>')
        wb_rels_lines.append('</Relationships>')

        zf.writestr("xl/workbook.xml", "\n".join(wb_lines))
        zf.writestr("xl/_rels/workbook.xml.rels", "\n".join(wb_rels_lines))

        # xl/worksheets/sheetN.xml
        for idx, (sname, rows) in enumerate(sheets_data.items(), start=1):
            ws_lines = [
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">',
                '  <sheetData>'
            ]
            for r_idx, row in enumerate(rows, start=1):
                ws_lines.append(f'    <row r="{r_idx}">')
                for c_idx, val in enumerate(row, start=1):
                    col_letter = chr(64 + c_idx) if c_idx <= 26 else f"A{chr(64 + c_idx - 26)}"
                    cell_ref = f"{col_letter}{r_idx}"
                    clean_val = str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    if isinstance(val, (int, float)):
                        ws_lines.append(f'      <c r="{cell_ref}"><v>{val}</v></c>')
                    else:
                        ws_lines.append(f'      <c r="{cell_ref}" t="inlineStr"><is><t>{clean_val}</t></is></c>')
                ws_lines.append('    </row>')
            ws_lines.append('  </sheetData>')
            ws_lines.append('</worksheet>')
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", "\n".join(ws_lines))

    out_p.write_bytes(buf.getvalue())
    return str(out_p)


def create_corporate_pptx(
    slides_data: List[Dict[str, Any]],
    output_path: str
) -> str:
    """
    Creates a valid 16:9 widescreen PowerPoint presentation (.pptx) with dark corporate theme.
    slides_data: [
        {"title": "Title", "subtitle": "Sub", "bullets": ["Point 1", "Point 2"], "metrics": [{"label": "Speedup", "value": "180x"}]}
    ]
    """
    out_p = Path(output_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # [Content_Types].xml
        ct = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
            '  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
            '  <Default Extension="xml" ContentType="application/xml"/>',
            '  <Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>'
        ]
        for idx in range(1, len(slides_data) + 1):
            ct.append(f'  <Override PartName="/ppt/slides/slide{idx}.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>')
        ct.append('</Types>')
        zf.writestr("[Content_Types].xml", "\n".join(ct))

        # _rels/.rels
        rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
            '  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/>\n'
            '</Relationships>'
        )
        zf.writestr("_rels/.rels", rels)

        # ppt/_rels/presentation.xml.rels
        pres_rels = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        ]
        pres_slides_entries = []
        for idx in range(1, len(slides_data) + 1):
            rid = f"rId{idx}"
            pres_rels.append(f'  <Relationship Id="{rid}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide{idx}.xml"/>')
            pres_slides_entries.append(f'    <p:sldId id="{255 + idx}" r:id="{rid}"/>')
        pres_rels.append('</Relationships>')
        zf.writestr("ppt/_rels/presentation.xml.rels", "\n".join(pres_rels))

        # ppt/presentation.xml (Widescreen 16:9: cx=12192000 cy=6858000)
        pres_xml = [
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
            '<p:presentation xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
            '  <p:sldMasterIdLst/>',
            '  <p:sldIdLst>',
            "\n".join(pres_slides_entries),
            '  </p:sldIdLst>',
            '  <p:sldSz cx="12192000" cy="6858000" type="screen16x9"/>',
            '</p:presentation>'
        ]
        zf.writestr("ppt/presentation.xml", "\n".join(pres_xml))

        # Slides
        for idx, s in enumerate(slides_data, start=1):
            title = str(s.get("title", "")).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            subtitle = str(s.get("subtitle", "")).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            bullets = s.get("bullets", [])

            bullet_paragraphs = []
            for b in bullets:
                b_clean = str(b).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                bullet_paragraphs.append(
                    f'      <a:p><a:r><a:rPr lang="en-US" sz="1800"><a:solidFill><a:srgbClr val="CCCCCC"/></a:solidFill></a:rPr><a:t>• {b_clean}</a:t></a:r></a:p>'
                )

            slide_xml = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
                '<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">\n'
                '  <p:cSld>\n'
                '    <p:spTree>\n'
                '      <p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>\n'
                '      <p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>\n'
                # Background shape (dark navy #0F172A)
                '      <p:sp><p:nvSpPr><p:cNvPr id="2" name="Bg"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>\n'
                '        <p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="12192000" cy="6858000"/></a:xfrm><a:solidFill><a:srgbClr val="0F172A"/></a:solidFill></p:spPr>\n'
                '      </p:sp>\n'
                # Title Box
                '      <p:sp><p:nvSpPr><p:cNvPr id="3" name="Title"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>\n'
                '        <p:spPr><a:xfrm><a:off x="800000" y="600000"/><a:ext cx="10500000" cy="1200000"/></a:xfrm></p:spPr>\n'
                '        <p:txBody><a:bodyPr/><a:lstStyle/>\n'
                f'          <a:p><a:r><a:rPr lang="en-US" sz="3200" b="1"><a:solidFill><a:srgbClr val="38BDF8"/></a:solidFill></a:rPr><a:t>{title}</a:t></a:r></a:p>\n'
                f'          <a:p><a:r><a:rPr lang="en-US" sz="1600"><a:solidFill><a:srgbClr val="94A3B8"/></a:solidFill></a:rPr><a:t>{subtitle}</a:t></a:r></a:p>\n'
                '        </p:txBody>\n'
                '      </p:sp>\n'
                # Bullets Box
                '      <p:sp><p:nvSpPr><p:cNvPr id="4" name="Content"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>\n'
                '        <p:spPr><a:xfrm><a:off x="800000" y="2000000"/><a:ext cx="10500000" cy="4000000"/></a:xfrm></p:spPr>\n'
                '        <p:txBody><a:bodyPr/><a:lstStyle/>\n'
                + "\n".join(bullet_paragraphs) +
                '\n        </p:txBody>\n'
                '      </p:sp>\n'
                '    </p:spTree>\n'
                '  </p:cSld>\n'
                '</p:sld>'
            )
            zf.writestr(f"ppt/slides/slide{idx}.xml", slide_xml)

    out_p.write_bytes(buf.getvalue())
    return str(out_p)
