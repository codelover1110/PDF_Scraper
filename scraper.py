import pdfplumber
import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import sys
import csv


class PdfExtractor:
    def __init__(self, pdf_path, pdf_columns):
        self.pdf_path = pdf_path
        self.pdf_name = os.path.basename(pdf_path)
        self.pdf_columns = pdf_columns
        positions = [column['x_pos'] for column in self.pdf_columns]
        positions = positions
        self.table_settings = {
            "vertical_strategy": "explicit",
            "horizontal_strategy": "text",
            "explicit_vertical_lines": positions,
        }

    def process(self):
        df_all = pd.DataFrame(columns=self.get_headers())
        with pdfplumber.open(self.pdf_path) as pdf:
            page_index = 0
            pages = pdf.pages
            for page_index in range(0, len(pages)):
                print('processing page:', page_index+1)
                df = self.extract_data(pages, page_index)
                if df is not None:
                    # modify df if there is empty code
                    # start_index = self.get_stop_index(df)
                    # if start_index > 0:
                    #     df = df.loc[start_index:]

                    # greedy to get data on next page
                    if self.is_need_more_data(df) and page_index<=len(pages)-2:
                        # print('need to get more for page at ', page_index, 'next page is ', page_index+1)
                        next_df = self.extract_data(pages, page_index+1)
                        df = self.merge_data(df, next_df)

                    page_index = page_index+1
                    df['Pdf_name'] = self.pdf_name
                    df['Page_number'] = str(page_index)
                    df_all = pd.concat([df_all, df], ignore_index=True)
                else:
                    # print('current page error:', page_index)
                    break

        return df_all

    def is_empty(self, cell_value):
        return pd.isna(cell_value) or (isinstance(cell_value, str) and cell_value.strip() == '')

    def get_stop_index(self, df):
        from_index = 0
        total_rows = len(df)
        to_index = None
        for index in range(from_index+1, total_rows):
            cell_value = df.loc[index, 'APT']
            if not self.is_empty(cell_value) or index==total_rows-1:
                to_index = index - 1
                break
        return to_index

    def merge_data(self, df, next_df):
        stop_index = self.get_stop_index(next_df)
        more_df = next_df.loc[0: stop_index-1]
        return pd.concat([df, more_df], ignore_index=True)

    def get_headers(self):
        headers = [column['header'] for column in self.pdf_columns]
        headers = headers[:-1]
        return headers

    def extract_data(self, pages, index):
        try:
            page = pages[index]
            page = page.crop((0, 66, page.width, page.height))
            pdf_data = page.extract_table(table_settings=self.table_settings)
            df = pd.DataFrame(pdf_data[4:], columns=self.get_headers())
            return df
        except:
            return None

    def is_need_more_data(self, data):
        # print(data.iloc[len(data)-1]['CODE'])
        last_row = len(data)-1
        return data.iloc[last_row]['CODE'] != '' and data.iloc[last_row]['CODE'] != 'Total:'

class CsvConverter:
    def __init__(self, csv_columns, code_columns):
        self.csv_columns = csv_columns
        self.code_columns = code_columns

    def execute(self, pdf_data):
        csv_df = pd.DataFrame(columns=self.csv_columns)

        from_index = 0
        while True:
            to_index = self.get_next_index(pdf_data, from_index)
            if not to_index:
                break
            df_rows = pdf_data.loc[from_index: to_index+1]
            csv_row = self.extract_csv_row(df_rows)
            csv_df = pd.concat([csv_df, csv_row], ignore_index=True)
            from_index = to_index+1


        csv_df = csv_df.iloc[:-1 , :]

        return csv_df

    def _get_bldg(self, pdf_rows):
        value = pdf_rows.iloc[0]['APT']
        parts = value.split(' - ')
        return parts[0]

    def _get_apt(self, pdf_rows):
        value = pdf_rows.iloc[0]['APT']
        parts = value.split(' - ')
        if len(parts)>1:
            return parts[1]
        else:
            return ''

    def _get_names(self, pdf_rows):
        names = [pdf_rows.iloc[index]['NAME'] for index in range(0, len(pdf_rows)-1) if pdf_rows.iloc[index]['NAME'] != ""]
        return " | ".join(names)

    def _get_total(self, pdf_rows):
        return self._get_by_code(pdf_rows, 'Total:')

    def _get_by_code(self, pdf_rows, code_name):
        code_list = pdf_rows['CODE'].tolist()

        if code_name in code_list:
            index = code_list.index(code_name)
            return pdf_rows.iloc[index]['LEASE_CHARGES']
        return ''

    def extract_csv_row(self, pdf_rows):
        row_data = {}
        row_data['Bldg'] = self._get_bldg(pdf_rows)
        row_data['Apt'] = self._get_apt(pdf_rows)
        row_data['Type'] = pdf_rows.iloc[0]['TYPE']
        row_data['Status'] = pdf_rows.iloc[0]['APT_STATUS']
        row_data['Names'] = self._get_names(pdf_rows)
        row_data['Resident_status'] = pdf_rows.iloc[0]['RESIDENT_STATUS']
        row_data['Sq_ft'] = pdf_rows.iloc[0]['SQUARE_FEET']
        row_data['Market_rent'] = pdf_rows.iloc[0]['MARKET_RENT']
        row_data['Gross_Possible'] = pdf_rows.iloc[0]['GROSS_POSSIBLE']
        row_data['Actual_Charge'] = pdf_rows.iloc[0]['ACTUAL_POTENTIAL_CHARGES']
        row_data['move_in_date'] = pdf_rows.iloc[0]['MIO_DATE']
        row_data['move_out_date'] = pdf_rows.iloc[1]['MIO_DATE']
        row_data['Lease_expire_date'] = pdf_rows.iloc[0]['LEASE_EXPIRES_TERM']
        row_data['term'] = pdf_rows.iloc[1]['LEASE_EXPIRES_TERM']
        row_data['ID'] = pdf_rows.iloc[1]['ID']
        row_data['ID_status'] = pdf_rows.iloc[1]['APT_STATUS']
        row_data['total'] = self._get_total(pdf_rows)
        row_data['Pdf_name'] = pdf_rows.iloc[0]['Pdf_name']
        row_data['Page_number'] = pdf_rows.iloc[0]['Page_number']

        for code in self.code_columns:
            row_data[code] = self._get_by_code(pdf_rows, code)

        csv_row = pd.DataFrame([row_data], columns=self.csv_columns)
        return csv_row

    def get_next_index(self, df, from_index):
        total_rows = len(df)
        if from_index >= total_rows:
            return None
        to_index = None
        for index in range(from_index+1, total_rows):
            cell_value = df.loc[index, 'APT']
            if not self.is_empty(cell_value) or index==total_rows-1:
                to_index = index - 1
                break
        return to_index

    def is_empty(self, cell_value):
        return pd.isna(cell_value) or (isinstance(cell_value, str) and cell_value.strip() == '')

class Scraper:
    def __init__(self, pdf_extractor, csv_converter):
        self.extractor = pdf_extractor
        self.csv_converter = csv_converter

    def run(self):
        pdf_data = self.extractor.process()
        csv_data = self.csv_converter.execute(pdf_data)
        return csv_data

class App:
    def __init__(self, pdf_path, csv_path):
        self.pdf_path = pdf_path
        self.csv_path = csv_path
        self.pdf_columns = [
            {'header': 'APT', 'x_pos': 21, },
            {'header': 'ID', 'x_pos': 67, },
            {'header': 'TYPE', 'x_pos': 84, },
            {'header': 'APT_STATUS', 'x_pos': 114, },
            {'header': 'NAME', 'x_pos': 132, },
            {'header': 'RESIDENT_STATUS', 'x_pos': 269, },
            {'header': 'SQUARE_FEET', 'x_pos': 289, },
            {'header': 'MARKET_RENT', 'x_pos': 330, },
            {'header': 'CODE', 'x_pos': 371, },
            {'header': 'LEASE_CHARGES', 'x_pos': 410, },
            {'header': 'GROSS_POSSIBLE', 'x_pos': 456, },
            {'header': 'ACTUAL_POTENTIAL_CHARGES', 'x_pos': 509, },
            {'header': 'MIO_DATE', 'x_pos': 553, },
            {'header': 'LEASE_EXPIRES_TERM', 'x_pos': 601, },
            {'header': 'LAST_POS', 'x_pos': 645, },
        ]

        self.code_columns = [
            'RENT',
            '1STFL',
            'BKL',
            'BKY',
            'CAMRE',
            'CARPT',
            'CRTSY',
            'DWS',
            'EMP',
            'END',
            'FLT',
            'FRPLC',
            'GASRE',
            'GLOBL',
            'HDWDF',
            'MTM',
            'PARK',
            'PARUP',
            'PETFE',
            'PETRT',
            'PTH',
            'SCB',
            'TCONC',
            'UPGRD',
            'WCB',
            'WUA/C',
        ]

        self.csv_columns = [
            'Bldg',
            'Apt',
            'Type',
            'Status',
            'Names',
            'Resident_status',
            'Sq_ft',
            'Market_rent',
            'Gross_Possible',
            'Actual_Charge',
            'move_in_date',
            'move_out_date',
            'Lease_expire_date',
            'term',
            'ID',
            'ID_status',
            'total',
        ]
        self.csv_columns = self.csv_columns + self.code_columns + ['Pdf_name', 'Page_number']

    def run(self):
        extractor = PdfExtractor(self.pdf_path, self.pdf_columns)
        converter = CsvConverter(self.csv_columns, self.code_columns)
        scraper = Scraper(extractor, converter)
        csv_data = scraper.run()
        csv_data.to_csv(self.csv_path, index=False)



# Function to merge CSV files in a folder
def merge_csv_files(folder_path, output_file):
    with open(output_file, 'w', newline='') as output_csv:
        writer = csv.writer(output_csv)
        write_header = True

        for root, _, files in os.walk(folder_path):
            for filename in files:
                if filename.endswith('.csv'):
                    file_path = os.path.join(root, filename)
                    with open(file_path, 'r', newline='') as input_csv:
                        reader = csv.reader(input_csv)
                        if write_header:
                            writer.writerow(next(reader))  # Write header only once
                            write_header = False
                        else:
                            next(reader)  # Skip header for subsequent files
                        for row in reader:
                            writer.writerow(row)

def clean_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete: {file_path} - Error: {e}")

def main():
    script_path = os.path.abspath(sys.argv[0])

    # Extract the folder path from the script path
    script_folder = os.path.dirname(script_path)


    pdf_folder = join(script_folder, 'pdf')
    csv_folder = join(script_folder, 'csv')
    clean_folder(csv_folder)

    files = os.listdir(pdf_folder)
    for filename in files:

        pdf_path = join(pdf_folder, filename)
        print('== Processing file: ', pdf_path)
        filename_without_extension = os.path.splitext(os.path.basename(pdf_path))[0]
        csv_path = join(csv_folder, filename_without_extension + '.csv')

        app = App(pdf_path, csv_path)
        app.run()

    merge_csv_files(csv_folder, 'output.csv')
    clean_folder(csv_folder)
    print("CSV files merged successfully.")

if __name__ == "__main__":
    main()
