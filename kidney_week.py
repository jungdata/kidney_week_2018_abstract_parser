import re
import pandas as pd
from tika import parser

raw = parser.from_file('KW18Abstracts.pdf')
raw_text = raw['content'][3580:12120360]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


# split by regex for abstract names
s = r'((TH|FR|SA)?-?(OR|PO|PUB)\d{3,4})\s'
split_abstracts = re.split(s, raw_text)

# Above regex splits into 4 sections by matching groups
#   poster_id
#   day
#   poster_type
#   poster_content

abstract_list = [sections for sections in chunks(split_abstracts[1:], 4)]
abstract_df = pd.DataFrame(abstract_list,
                           columns=['poster_id', 'day', 'poster_type', 'poster_content'])


def section_split(text):
    sections = r'(Background|Methods|Results|Conclusions|Funding):\s'
    sectioned_list = re.split(sections, text)

    # Title actually contains title and authorlist...
    title_dict = [{'Session': re.split(r'\n', title.rstrip())[1],
                   'TitleAuthor': " ".join(re.split(r'\n', title.rstrip())[3:])} for title in sectioned_list[0:1]]

    content_dict = {chunk[0]: chunk[1].rstrip() for chunk in chunks(sectioned_list[1:], 2)}
    # Returns dictionary of section : text
    return {**title_dict[0], **content_dict}

# Some may prefer this dictionary... but for pure python work would convert
abstract_df['poster_content'] = abstract_df.poster_content.apply(lambda x: section_split(x))

# Some may find wide form better
wide_abstract_df = pd.concat([abstract_df.drop(['poster_content'], axis=1),
                              abstract_df['poster_content'].apply(pd.Series)], axis=1)

# I like the melted form better
abstract_df = pd.melt(wide_abstract_df,
                      id_vars=['poster_id', 'day', 'poster_type'],
                      var_name="section",
                      value_name='text',
                      value_vars=['TitleAuthor', 'Session', 'Background', 'Methods',
                                  'Conclusions', 'Results', 'Funding']).reset_index(drop=True)


######################################################################
# One helper function
######################################################################

def get_matching_abstracts(search_str, section=None, return_match_sections_only=False):
    if section:
        results = abstract_df[(abstract_df['section'] == section) &
                              (abstract_df.text.str.contains(search_str, flags=re.IGNORECASE))]
    else:
        results = abstract_df[~(abstract_df.section.isna()) &
                              (abstract_df.text.str.contains(search_str, flags=re.IGNORECASE))]

    matched_poster_ids = sorted(list(set(results.poster_id.tolist())))

    if not return_match_sections_only:
        results = abstract_df[abstract_df.poster_id.isin(matched_poster_ids)]
        print('{} has {} abstracts: {}'.format(search_str, len(matched_poster_ids), matched_poster_ids))
    print(matched_poster_ids)
    return results.sort_values('poster_id').reset_index(drop=True)


######################################################################
# Default search: returns all sections of the presentation
#   'section' will return only matching section
#   'return_matching_sections_only' = will override this behavior
#
# --------------------------------------------------------------------
# Possible Sections
#   'TitleAuthor', 'Session', 'Background', 'Methods',
#   'Conclusions', 'Results', 'Funding'
#
######################################################################

get_matching_abstracts('AVF')
get_matching_abstracts('AVF', section='Funding')

# By default returns all matching sections, but can just reutn matched sections only
get_matching_abstracts('Machine Learning', 'TitleAuthor')
get_matching_abstracts('Machine Learning', 'TitleAuthor', return_match_sections_only=True)

# Visit our posters at pulseData
get_matching_abstracts('pulseData', 'TitleAuthor', return_match_sections_only=True)
