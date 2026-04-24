import csv
import sqlite3
import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from genealogy.cli import run


SAMPLE_VDX = """\
<VisioDocument>
  <SolutionXML>
    <SolutionModel>
      <mstns:SolutionModelData xmlns:mstns="urn:schemas-microsoft-com:office:office">
        <mstns:Position ID="{ROOT}">
          <mstns:Name>Jan р. 01.02.1900 + Anna Kowalska (л.262)</mstns:Name>
          <mstns:Title>Jan b. 01.02.1900 + Anna Kowalska</mstns:Title>
        </mstns:Position>
        <mstns:Position ID="{CHILD}">
          <mstns:Name>Piotr кр.03.04.1920 умер 05.06.1980 archive note</mstns:Name>
          <mstns:Title>Piotr</mstns:Title>
        </mstns:Position>
        <mstns:ReportsTo SourceObject="{ROOT}" TargetObject="{CHILD}"/>
      </mstns:SolutionModelData>
    </SolutionModel>
  </SolutionXML>
</VisioDocument>
"""


class ConverterTests(unittest.TestCase):
    def test_import_vdx_splits_spouses_and_preserves_links(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            vdx = root / "sample.vdx"
            db = root / "genealogy.sqlite"
            review = root / "review.csv"
            vdx.write_text(SAMPLE_VDX, encoding="utf-8")

            run(["import-vdx", str(vdx), "--db", str(db), "--review", str(review)])

            with sqlite3.connect(db) as conn:
                conn.row_factory = sqlite3.Row
                persons = conn.execute(
                    "select person_id, source_visio_id, display_name, birth, christening, death, notes from persons order by person_id"
                ).fetchall()
                families = conn.execute("select * from families").fetchall()
                children = conn.execute("select * from family_children").fetchall()
                issues = conn.execute("select issue_type from import_issues order by issue_type").fetchall()

            self.assertEqual([p["display_name"] for p in persons], ["Jan", "Anna Kowalska", "Piotr"])
            self.assertEqual(persons[0]["birth"], "01.02.1900")
            self.assertIn("л.262", persons[1]["notes"])
            self.assertEqual(persons[2]["christening"], "03.04.1920")
            self.assertEqual(persons[2]["death"], "05.06.1980")
            self.assertEqual(len(families), 1)
            self.assertEqual(len(children), 1)
            self.assertEqual(children[0]["child_person_id"], "P0003")
            self.assertTrue(review.exists())
            self.assertIn("note_fragment", [row["issue_type"] for row in issues])

    def test_media_manifest_resolves_unique_name_hint_and_exporters_include_media(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            vdx = root / "sample.vdx"
            db = root / "genealogy.sqlite"
            manifest = root / "photos.csv"
            ged = root / "out.ged"
            html = root / "site" / "index.html"
            generated = root / "out.vdx"
            vdx.write_text(SAMPLE_VDX, encoding="utf-8")
            manifest.write_text(
                textwrap.dedent(
                    """\
                    person_id,name_hint,file,title,notes
                    ,Piotr,piotr.jpg,Portrait,Scanned album photo
                    """
                ),
                encoding="utf-8",
            )

            run(["import-vdx", str(vdx), "--db", str(db)])
            run(["import-media", str(manifest), "--db", str(db), "--media-dir", "media"])
            run(["export-gedcom", "--db", str(db), "--out", str(ged)])
            run(["export-html", "--db", str(db), "--out", str(html)])
            run(["export-vdx", "--db", str(db), "--out", str(generated)])

            ged_text = ged.read_text(encoding="utf-8")
            html_text = html.read_text(encoding="utf-8")
            vdx_text = generated.read_text(encoding="utf-8")

            self.assertIn("0 @P0003@ INDI", ged_text)
            self.assertIn("2 FILE media/piotr.jpg", ged_text)
            self.assertIn("tree-data", html_text)
            self.assertIn("media/piotr.jpg", html_text)
            self.assertIn("<VisioDocument", vdx_text)
            self.assertIn("Dynamic connector", vdx_text)

    def test_html_export_builds_svg_family_tree_view(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            vdx = root / "sample.vdx"
            db = root / "genealogy.sqlite"
            html = root / "site" / "index.html"
            vdx.write_text(SAMPLE_VDX, encoding="utf-8")

            run(["import-vdx", str(vdx), "--db", str(db)])
            run(["export-html", "--db", str(db), "--out", str(html)])

            html_text = html.read_text(encoding="utf-8")

            self.assertIn('<svg id="tree-svg"', html_text)
            self.assertIn('familyNodes', html_text)
            self.assertIn('viewport"', html_text)
            self.assertIn('zoom-in', html_text)
            self.assertIn('reset-view', html_text)
            self.assertIn('Jan', html_text)
            self.assertIn('Anna Kowalska', html_text)
            self.assertIn('1900', html_text)
            self.assertNotIn('button class="toggle"', html_text)
            self.assertNotIn('renderPerson(', html_text)


if __name__ == "__main__":
    unittest.main()
