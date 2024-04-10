import pandas as pd
import csv
import os
import re
import random
from faker import Faker
import random
import numpy as np

# Utils

def export_to_csv(data, output_file):
    fieldnames = list(data.keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # zip every key values and write into csv
        for row_data in zip(*data.values()):
            writer.writerow({field: value for field, value in zip(fieldnames, row_data)})



# Node

####################################################################################
#################  Paper       ###################################################
####################################################################################
def extract_paper_from_csv(input_file_path):
    papers = {
        "title": [],
        "abstract": [],
        "pages": [],
        "DOI": [],
        "link": [],
        "year": []
    }

    with open(input_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader):
            papers["title"].append(row["Title"])
            papers["abstract"].append(row["Abstract"])
            papers["pages"].append(f"{row['Page start']}-{row['Page end']}")
            papers["DOI"].append(row["DOI"])
            papers["link"].append(row["Link"])
            papers["year"].append(str(row["Year"]))

    return papers

def extract_paper(input_file, output_path):
    papers_data = extract_paper_from_csv(input_file_path=input_file)
    paper_name = 'paper.csv'
    output_file_path = os.path.join(output_path, paper_name)
    export_to_csv(papers_data, output_file_path)
    print("paper.csv write to:", output_file_path)


####################################################################################
#################  Journal       ###################################################
####################################################################################
# Print Journal table
def extract_journals_from_csv(input_file):
    journals = {"journal_name": [], "year": []}

    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            conference_name = row["Conference name"]
            if not conference_name:  # if Conference name IS empty，regarding as journal
                journals["journal_name"].append(row["Source title"])
                journals["year"].append(str(row["Year"]))
    # deduplicate
    unique_journal = []
    seen = set()
    for journal in zip(journals["journal_name"], journals["year"]):
        if journal not in seen:
            seen.add(journal)
            unique_journal.append({"journal_name": journal[0], "year": journal[1]})
    return unique_journal
def export_to_csv_list(data, output_file):
    if not data:
        return
    
    fieldnames = list(data[0].keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row_data in data:
            writer.writerow(row_data)

def extract_journal(input_file, output_path):
    journal_data = extract_journals_from_csv(input_file)
    unique_journal_names = set(entry['journal_name'] for entry in journal_data)
    
    # Write unique journal names to 'journal.csv'
    journal_name_file = 'journal.csv'
    journal_name_file_path = os.path.join(output_path, journal_name_file)
    with open(journal_name_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["journal_name"])  # Write header
        for journal_name in unique_journal_names:
            writer.writerow([journal_name])
    
    print("Unique journal names saved to:", journal_name_file_path)

# extract journal information
def extract_journal_in_year(input_file, output_path):
    journal_data = extract_journals_from_csv(input_file)
    
    journal_name = 'journal_in_year.csv'
    # write journal.csv
    output_file_path = os.path.join(output_path, journal_name)
    export_to_csv_list(journal_data, output_file_path)
    print("journal_in_year.csv write to:", output_file_path)


####################################################################################
#################  Proceedings       ###################################################
####################################################################################
# def extract_proceeding_from_csv(file_path):
#     proceeding = {"proceeding_name": []}

#     with open(file_path, newline='', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             conference_name = row["Conference name"]
#             if conference_name:  # if Conference name NOT empty，regarding as proceeding
#                 proceeding["proceeding_name"].append(row["Source title"])
    
#     # Deduplicate
#     proceeding["proceeding_name"] = list(set(proceeding["proceeding_name"]))
#     return proceeding

# def extract_proceeding(input_file, output_path):
#     # extract proceeding info
#     proceeding_data = extract_proceeding_from_csv(input_file)
#     proceeding_name = 'proceeding.csv'
#     # write proceeding.csv 
#     output_file_path = os.path.join(output_path, proceeding_name)
#     export_to_csv(proceeding_data, output_file_path)
#     print("proceeding.csv write to:", output_file_path)

####################################################################################
#################  Conference       ################################################
####################################################################################
def extract_conference_name(full_name):
    # 使用正则表达式来提取会议名称中的中间部分
    match = re.match(r'^.*?\s([^\d]+)\s\d{4}$', full_name)
    if match:
        return match.group(1)
    else:
        return full_name
    
def extract_conference_from_csv(input_file):
    conferences = {"name": [], "year": [], "city": [], "proceeding_name": []}

    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            conference_name = row["Conference name"].strip()
            if conference_name:
                name = extract_conference_name(conference_name)  # 提取中间部分
                conferences["name"].append(name)
                # year_match = re.search(r'\d{4}', row["Conference date"])
                # year = year_match.group(0) if year_match else None
                conferences["year"].append(str(row["Year"]))
                conferences["city"].append(row["Conference location"].strip())
                conferences["proceeding_name"].append(row["Conference name"].strip())
                # conferences["proceeding_name"].append(row["Source title"].strip())
    # deduplicate
    unique_conferences = []
    seen = set()
    for conference in zip(conferences["name"], conferences["year"], conferences["city"], conferences["proceeding_name"]):
        if conference not in seen:
            seen.add(conference)
            unique_conferences.append({"name": conference[0], "year": conference[1], "city": conference[2], "proceeding_name": conference[3]})
    return unique_conferences

def extract_conference(input_file, output_path):
    conference_data = extract_conference_from_csv(input_file)
    unique_conference_names = set(entry['name'] for entry in conference_data)
    
    # Write unique conference names to 'conference.csv'
    conference_name_file = 'conference.csv'
    conference_name_file_path = os.path.join(output_path, conference_name_file)
    with open(conference_name_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["name"])  # Write header
        for conference_name in unique_conference_names:
            writer.writerow([conference_name])
    
    print("Unique conference names saved to:", conference_name_file_path)

def extract_proceeding(input_file, output_path):
    proceeding_data = extract_conference_from_csv(input_file)
    unique_proceeding_names = set(entry['proceeding_name'] for entry in proceeding_data)
    
    # Write unique proceeding names to 'proceeding.csv'
    proceeding_name_file = 'proceeding.csv'
    proceeding_name_file_path = os.path.join(output_path, proceeding_name_file)
    with open(proceeding_name_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["proceeding_name"])  # Write header
        for proceeding_name in unique_proceeding_names:
            writer.writerow([proceeding_name])
    
    print("Unique proceeding names saved to:", proceeding_name_file_path)

def extract_proceeding_in_year(input_file, output_path):
    pro_in_year_data = extract_conference_from_csv(input_file)
    unique_pro_in_year = set((entry['proceeding_name'], entry['year']) for entry in pro_in_year_data)
    
    # Write unique proceeding names and years to 'proceeding_in_year.csv'
    pro_in_year_file = 'proceeding_in_year.csv'
    pro_in_year_file_path = os.path.join(output_path, pro_in_year_file)
    with open(pro_in_year_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["proceeding_name", "year"])  # Write header
        writer.writerows(unique_pro_in_year)
    
    print("Unique proceeding_in_year saved to:", pro_in_year_file_path)

def export_conference_to_csv(data, output_file):
    if not data:
        return

    fieldnames = list(data[0].keys())

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row_data in data:
            writer.writerow(row_data)

def extract_conference_detail(input_file, output_path):
    # extract
    conference_data = extract_conference_from_csv(input_file=input_file)
    conference_name = 'conference_detail.csv'
    # write csv
    output_file_path = os.path.join(output_path, conference_name)
    export_conference_to_csv(conference_data, output_file_path)
    print("conference_detail.csv write to:", output_file_path)


# Relationship
    
####################################################################################
#################  conference_belong_to_proceedings       ################################################
####################################################################################

# def extract_conference_proceeding_from_csv(input_file):
#     conference_proceeding = {"start_id": [], "end_id": []}
#     seen_combinations = set()

#     with open(input_file, newline='', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             conference_name = row["Conference name"].strip()
#             source_title = row["Source title"].strip()
#             if conference_name and source_title:
#                 name = extract_conference_name(conference_name)
#                 combined_key = (name, source_title)
#                 if combined_key not in seen_combinations:
#                     seen_combinations.add(combined_key)
#                     conference_proceeding["start_id"].append(name)
#                     conference_proceeding["end_id"].append(source_title)


# def extract_conference_proceeding(input_file,output_path):
#     # extract
#     conference_proceeding_data = extract_conference_proceeding_from_csv(input_file)

#     # write csv
#     output_file_name = "conference_belong_to_proceeding.csv"
#     output_file_path = os.path.join(output_path, output_file_name)
#     export_to_csv(conference_proceeding_data, output_file_path)

#     print("conference_proceeding.csv  write to:", output_file_path)



####################################################################################
#################  paper_belong_to_conferences       ################################################
####################################################################################

def extract_paper_conference_from_csv(file_path):
    paper_conference = {"start_id": [], "end_id": []}
    seen_combinations = set()

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            DOI = row["DOI"].strip()
            conference_name = row["Conference name"].strip()
            # source_title = row["Source title"].strip()
            if conference_name:
                name = extract_conference_name(conference_name)
                combined_key = (DOI, name)
                if combined_key not in seen_combinations:
                    seen_combinations.add(combined_key)
                    paper_conference["start_id"].append(DOI)
                    paper_conference["end_id"].append(name)

    return paper_conference

def extract_paper_conference(input_file, output_path):
    #  extract
    paper_conference_data = extract_paper_conference_from_csv(input_file)

    # write csv
    output_file_name = "paper_presented_in_conference.csv"
    output_file_path = os.path.join(output_path, output_file_name)
    export_to_csv(paper_conference_data, output_file_path)

    print("paper_conference.csv  write to:", output_file_path)

####################################################################################
#################  paper_belong_to_journal       ################################################
####################################################################################
def extract_paper_journal_from_csv(input_file):
    paper_journal = {"start_id": [], "end_id": []}
    seen_combinations = set()

    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            DOI = row["DOI"].strip()
            conference_name = row["Conference name"].strip()
            source_title = row["Source title"].strip()
            if not conference_name:
                combined_key = (DOI, source_title)
                if combined_key not in seen_combinations:
                    seen_combinations.add(combined_key)
                    paper_journal["start_id"].append(DOI)
                    paper_journal["end_id"].append(source_title)

    return paper_journal

def extract_paper_journal(input_file, output_path):
    # extract info
    paper_journal_data = extract_paper_journal_from_csv(input_file)

    # write CSV
    output_file_name = "paper_belong_to_journal.csv"
    output_file_path = os.path.join(output_path, output_file_name)
    export_to_csv(paper_journal_data, output_file_path)

    print("paper_journal.csv write to:", output_file_path)


####################################################################################
#################  paper_cite_paper       ###########################################
####################################################################################
### Assumption is paper is cited by [0-50] papers

def generate_paper_cite_paper(input_file):
    paper_cite_paper = {"start_id": [], "end_id": []}
    
    # open sample.csv
    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        dois = [row["DOI"].strip() for row in reader if row["DOI"].strip()]

    # for every DOI
    for doi in dois:
        # randomly select DOI which are different from the current DOI
        end_dois = random.sample([d for d in dois if d != doi], min(random.randint(0, 50), len(dois) - 1))
        
        # add current DOI to start_id 
        paper_cite_paper["start_id"].extend([doi] * len(end_dois))
        
        # add randomized DOI to end_id
        paper_cite_paper["end_id"].extend(end_dois)
    return(paper_cite_paper)


def extract_cite(input_file, output_path):
    # extract cite info
    paper_cite_paper = generate_paper_cite_paper(input_file)

    # write paper_cite_paper.CSV
    output_file_name = "paper_cite_paper.csv"
    output_file_path = os.path.join(output_path, output_file_name)
    export_to_csv(paper_cite_paper, output_file_path)

    print("paper_cite_paper.csv write to:", output_file_path)


####################################################################################
#################  author + author_writes      #####################################
####################################################################################

def extract_author_and_write(input_file, output_path):
    df = pd.read_csv(input_file,low_memory=False)

    # Split values in "Authors", "Author(s) ID", and "Authors with affiliations" columns
    df["author_name"] = df["Authors"].str.split("; ")
    df["author_id"] = df["Author(s) ID"].str.split("; ")
    df["author_affiliation"] = df["Authors with affiliations"].str.split("; ")

    # Create an empty list to store data
    data = []
    aw = []

    # Iterate through each row of the original DataFrame and append data to the list
    for index, row in df.iterrows():
        for i in range(len(row["author_name"])):
            author_affiliation = row["author_affiliation"][i]
            affiliation = ",".join(author_affiliation.split(",")[1:])
            data.append({
                "author_id": row["author_id"][i],
                "author_name": row["author_name"][i],
                "author_affiliation": affiliation
            })
            # try:
                # author_affiliation = row["author_affiliation"][i]
                # affiliation = ",".join(author_affiliation.split(",")[1:])
                # data.append({
                #     "author_id": row["author_id"][i],
                #     "author_name": row["author_name"][i],
                #     "author_affiliation": affiliation
                # })
            # except IndexError:
            #     print(f"IndexError occurred at index: {index}")

    for index, row in df.iterrows():
        for i in range(len(row["author_name"])):
            if i == 0:
                aw.append({
                    "author_id": row["author_id"][i],
                    "paper_id": row["DOI"],
                    "corresponding": True,
                })
            else:
                aw.append({
                    "author_id": row["author_id"][i],
                    "paper_id": row["DOI"],
                    "corresponding": False,
                })           

    # Create a new DataFrame from the list of data
    new_df = pd.DataFrame(data)

    # Remove duplicates from the new DataFrame
    new_df = new_df.drop_duplicates()

    aw_df = pd.DataFrame(aw)
    # Write the new DataFrame to a new CSV file
    new_df.to_csv(os.path.join(output_path, "authors.csv"), index=False)
    aw_df.to_csv(os.path.join(output_path, "author_write.csv"), index=False)
    print("authors.csv written, author_write.csv_written")


####################################################################################
#################  keywords + paper_has_keywords      ##############################
####################################################################################


def extract_keywords(input_file, output_path):
    # Keywords
    # Read the original CSV file
    df = pd.read_csv(input_file,low_memory=False)

    df.dropna(subset=['Author Keywords'], inplace=True)
    # Split values in "Authors", "Author(s) ID", and "Authors with affiliations" columns
    df["keywords"] = df["Author Keywords"].str.split("; ")


    # Create an empty list to store data
    data = []

    # Iterate through each row of the original DataFrame and append data to the list
    for index, row in df.iterrows():
        if len(row["keywords"]) != 0:
            for i in range(len(row["keywords"])):
                data.append({
                    "keywords": row["keywords"][i]
                })

    # Create a new DataFrame from the list of data
    new_df = pd.DataFrame(data)

    # Remove duplicates from the new DataFrame
    new_df = new_df.drop_duplicates()

    # Write the new DataFrame to a new CSV file
    new_df.to_csv(os.path.join(output_path, "keywords.csv"), index=False)
    print("keywords written")


def extract_paper_has_keywords(input_file, output_path):
    # Keywords
    # Read the original CSV file
    df = pd.read_csv(input_file,dtype={'Index Keywords': 'str'}, low_memory=False)

    df.dropna(subset=['Author Keywords'], inplace=True)
    # Split values in "Authors", "Author(s) ID", and "Authors with affiliations" columns
    df["keywords"] = df["Author Keywords"].str.split("; ")


    # Create an empty list to store data
    data = []

    # Iterate through each row of the original DataFrame and append data to the list
    for index, row in df.iterrows():
        if len(row["keywords"]) != 0:
            for i in range(len(row["keywords"])):
                data.append({
                    "paper_id": row["DOI"],
                    "keywords": row["keywords"][i]
                })

    # Create a new DataFrame from the list of data
    new_df = pd.DataFrame(data)

    # Write the new DataFrame to a new CSV file
    new_df.to_csv(os.path.join(output_path, "paper_has_keywords.csv"), index=False)
    print("paper_has_keywords written")



####################################################################################
#################  author_review      ##############################################
####################################################################################


def generate_random_text(length):
    fake = Faker()
    random_text = fake.text(max_nb_chars=length)
    return random_text


def extract_review(input_file, output_path):
    df = pd.read_csv(input_file, low_memory=False)
    paper_author_path = os.path.join(output_path, "author_write.csv")
    paper_author = pd.read_csv(paper_author_path)
    # paper_author = pd.read_csv("processed_data/author_write.csv")
    papers = list(df["DOI"])
    # Create an empty list to store data
    data = []

    for paper in papers:
        reviewer_bank = list(paper_author[paper_author["paper_id"]!=paper]["author_id"].drop_duplicates())
        reviewers = random.sample(reviewer_bank, 3)
        for reviewer in reviewers:
            data.append({
                "paper_id": paper,
                "reviewer_id": reviewer,
                "review_content": generate_random_text(50)
            })

    new_df = pd.DataFrame(data)
    new_df.to_csv(os.path.join(output_path, "author_review.csv"), index=False)
    print("author_review written")

def extract_year(input_file, output_path):
    df = pd.read_csv(input_file, low_memory=False)  # 指定sep=None自动识别分隔符
    df.rename(columns={"Year": "year"}, inplace=True)
    df_year = df["year"]
    df_year = df_year.drop_duplicates()
    df_year.to_csv(os.path.join(output_path, "year.csv"), index=True, sep=",")  # 指定逗号分隔符并保留索引
    print("Extract year success!")

# def get_year(input_file):
#     year = {"year": []}
#     with open(input_file, newline='', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             year["year"].append(row["Year"])
#     year["year"] = list(set(year["year"]))
#     return year
# def extract_year(input_file, output_path):
#     year_data = get_year(input_file)
#     year_name = 'year.csv'
#     output_file_path = os.path.join(output_path, year_name)
#     export_to_csv(year_data, output_file_path)
#     print("year.csv write to:", output_file_path)


if __name__ == "__main__":
    # input_path = "/Users/wanglinhan/Desktop/BDMA/UPC/SDM/labs/bdma-upc-sdm-lab1/raw_data"
    input_path = "/Users/zzy13/Desktop/Classes_at_UPC/SDM_Semantic_data_management/Lab_1/Codes/Data/Row_data"
    input_name = "scopus_500.csv"
    input_file = os.path.join(input_path, input_name)
    # output_path = "/Users/wanglinhan/Desktop/BDMA/UPC/SDM/labs/bdma-upc-sdm-lab1/processed_data"
    output_path = "/Users/zzy13/Desktop/Classes_at_UPC/SDM_Semantic_data_management/Lab_1/Codes/Data/Processed_data"
    extract_paper(input_file=input_file, output_path=output_path)
    extract_journal(input_file=input_file, output_path=output_path)
    extract_journal_in_year(input_file=input_file, output_path=output_path)
    extract_proceeding(input_file=input_file, output_path=output_path)
    extract_conference(input_file=input_file, output_path=output_path)
    extract_conference_detail(input_file=input_file, output_path=output_path)
    extract_paper_conference(input_file=input_file, output_path=output_path)
    extract_paper_journal(input_file=input_file, output_path=output_path)
    extract_cite(input_file=input_file, output_path=output_path)
    extract_author_and_write(input_file=input_file, output_path=output_path)
    extract_keywords(input_file=input_file, output_path=output_path)
    extract_paper_has_keywords(input_file=input_file, output_path=output_path)
    extract_review(input_file=input_file, output_path=output_path)
    extract_year(input_file=input_file, output_path=output_path)
    extract_proceeding_in_year(input_file=input_file, output_path=output_path)

    # extract_conference_proceeding(input_file=input_file, output_path=output_path)