from agyw import AgywPrev, AgywPrevCommune
from re import sub
from openpyxl import Workbook
from openpyxl.styles.colors import Color
from openpyxl.styles.fills import PatternFill
from openpyxl.styles import Font
from openpyxl.styles.alignment import Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles.borders import Border, Side

# constante openpyxl design reusable elements
my_blue = Color(rgb="0000FF")
blue_fill = PatternFill(patternType="solid",fgColor=my_blue)
my_red = Color(rgb="FF0000")
red_fill = PatternFill(patternType="solid",fgColor=my_red)
bold_12 = Font(size=12,bold=True)
text_datim_title = Alignment(horizontal="center",
                            vertical="bottom",
                            wrapText=True)
thin_border = Border(left=Side(style='thin'), 
                     right=Side(style='thin'), 
                     top=Side(style='thin'), 
                     bottom=Side(style='thin'))

# Name Handler
def name_handler(s):
    s = sub(r"[^\w\s]", '', s)
    # Replace all runs of whitespace with a single dash
    s = sub(r"\s+", '-', s)
    return s


# init workbook
def init_wb():
    """This function will be responsible for initiate the workbook"""
    wb = Workbook()
    return wb

