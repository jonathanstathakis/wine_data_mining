import pandas as pd
import pdfplumber


class Extractor:
    def __init__(self):
        self.curr_section = ""
        self.curr_subsection = ""
        self.curr_subsubsection = ""
        self.curr_text = ""

    def _extract_words(self, page):
        page_words = page.extract_words(extra_attrs=["fontname", "size", "bottom"])
        numbered_words = []
        for x in page_words:
            x["page_num"] = page.page_number
            numbered_words.append(x)

        sorted_words = sorted(numbered_words, key=lambda x: (x["bottom"], x["x0"]))

        return sorted_words

    def _label_words_by_line_number(self, words):
        line_num = -1
        curr_top = 0

        numbered_lines = []
        for x in words:
            top = round(x["top"], 2)
            if top > curr_top:
                line_num += 1
                curr_top = top

            x["line_num"] = line_num
            numbered_lines.append(x)
        return numbered_lines

    def _merge_lines(self, lines):
        # max number of lines

        line_num_max = lines[-1]["line_num"]

        # now merge same lines
        merged_lines = []

        line_num_gen = range(0, line_num_max + 1)

        merged_lines = [
            {"merged_text": "", "words": [], "line_num": num} for num in line_num_gen
        ]

        for x in lines:
            num = x["line_num"]
            if merged_lines[num]["merged_text"]:
                merged_lines[num]["merged_text"] += " " + x["text"]
            else:
                merged_lines[num]["merged_text"] = x["text"]
            merged_lines[num]["words"].append(x)

        # sort the words
        lines_sorted = [x for x in merged_lines]

        for i, line in enumerate(merged_lines):
            words = sorted(line["words"], key=lambda d: d["x0"])
            lines_sorted[i]["words"] = words

        return lines_sorted

    def label_lines_above_rectangle(self, word_0, rectangles, line_height_tol):
        line_above_rectangle = False
        if not rectangles:
            return False
        else:
            for rectangle in rectangles:
                rect_top = rectangle["top"]
                height_line_oen_above_rect = rect_top - line_height_tol
                line_above_rectangle = (rect_top > word_0["bottom"]) & (
                    height_line_oen_above_rect < word_0["bottom"]
                )
                if line_above_rectangle:
                    break

        return line_above_rectangle

    def _label_lines_by_section(self, lines, line_height_tol, rectangles):
        labelled_lines = []
        for line in lines:
            word_0 = line["words"][0]
            if word_0["x0"] > 200:
                line["line_type"] = "section"
            elif self.label_lines_above_rectangle(word_0, rectangles, line_height_tol):
                line["line_type"] = "subsection"
            elif "Italic" in word_0["fontname"]:
                line["line_type"] = "subsubsection"
            else:
                line["line_type"] = "text"

            labelled_lines.append(line)

        return labelled_lines

    def _tabulate_lines_by_sections(self, lines):
        columns = ["section", "subsection", "subsubsection", "line"]
        rows = []

        for line in lines:
            text = line["merged_text"]
            line_type = line["line_type"]
            if line_type == "section":
                self.curr_section = text
            elif line_type == "subsection":
                self.curr_subsection = text
            elif line_type == "subsubsection":
                self.curr_subsubsection = text
            else:
                curr_text = text
                rows.append(
                    (
                        self.curr_section,
                        self.curr_subsection,
                        self.curr_subsubsection,
                        curr_text,
                    )
                )
        return pd.DataFrame.from_records(rows, columns=columns)

    def _remove_continued(self, labelled_lines):
        """
        remove lines if contain "Continued" as is superfluous noise.
        """

        continued_removed = []

        for line in labelled_lines:
            if "Continued" in line["merged_text"]:
                pass
            else:
                continued_removed.append(line)
        return continued_removed

    def extract_page(self, page, line_height_tol):
        words = self._extract_words(page)
        rectangles = page.rects
        words_line_labelled = self._label_words_by_line_number(words)
        line_merged = self._merge_lines(words_line_labelled)
        section_labelled = self._label_lines_by_section(
            line_merged, line_height_tol, rectangles
        )
        continued_removed = self._remove_continued(section_labelled)
        df = self._tabulate_lines_by_sections(continued_removed)

        return df


fpath = "./scraping_producers/bennelong_wine_list.pdf"
pdf = pdfplumber.open(fpath)

extractor = Extractor()

dfs = []
for page in pdf.pages[26:27]:
    df_ = extractor.extract_page(page=page, line_height_tol=15)
    df_["page_num"] = page.page_number
    dfs.append(df_)

merged_df = pd.concat(dfs)

import duckdb as db

conn = db.connect()
print(conn.sql("select * from merged_df"))
