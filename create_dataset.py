import pandas as pd
from itertools import count, combinations
import numpy as np
import time
import sys
import json
from jsmin import jsmin
from collections import Counter

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(message)s')

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)

logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

DEFAULT_SETTINGS = dict(
    # path to panel structure excel file
    path_panel='settings/panel.xlsx',

    # name of sheets in panel structure excel file
    sheet_persons='persons',
    sheet_panel_entities='panel_entities',
    sheet_companies='companies',
    sheet_groups='groups',
    sheet_questions='data dictionary',
    sheet_scales='scales',

    # path to survey data - {} will be replaced with year (YYYY)
    path_results='survey-results/results_{}.xlsx',

    # path to resulting output file
    path_output='datasets/dnp_panel-data_{}-{}',

    # years to include
    years=[2013, 2014, 2015, 2016, 2017, 2018, 2019],

    # exclude deprecated variables
    exclude_deprecated=True,

    # exclude variables with personal information
    exclude_personal=True,

    # exclude variables with auxiliary information
    exclude_auxiliary=True,

    # exclude variables from special sections
    exclude_special_sections=True,

    # exclude din meta data
    exclude_din_meta_data=True,

    # exclude variables with less x years in which they were part of the questionnaire
    exclude_years_min=0,

    # allow missing observations: include panel entities which did not complete all specified years
    allow_missing=True,

    # save the results to an excel file
    save_output=True
)


class DataSetCreator(object):

    def __init__(self, settings=DEFAULT_SETTINGS):
        self.settings = settings
        self.years = self.settings['years']

    def load_files(self):
        try:
            self.questions = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_questions'])
            self.scales = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_scales']).groupby('scale')
            self.data = {k: pd.read_excel(
                self.settings['path_results'].format(k)) for k in self.settings['years']}
            self.persons = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_persons'])
            self.companies = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_companies'])
            self.panel_entities = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_panel_entities'])
            self.groups = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_groups'])
            self.selection = pd.read_excel(
                self.settings['path_panel'], sheet_name=self.settings['sheet_selection'])
        except FileNotFoundError as e:
            logger.error(e)
            exit(0)

    def prepare_questions(self):
        query = 'name != "NaN"'
        query += ' and not deprecated' if self.settings['exclude_deprecated'] else ""
        query += ' and not personal_data' if self.settings['exclude_personal'] else ""
        query += ' and not auxiliary' if self.settings['exclude_auxiliary'] else ""
        query += ' and not name.str.startswith("din_")' if self.settings['exclude_din_meta_data'] else ""
        query += ' and num_years_observed >= {}'.format(self.settings['exclude_years_min'])

        self.single_query = query

        self.panel_query = self.single_query
        self.panel_query += ' and special_section_year == "NaN"' if self.settings['exclude_special_sections'] else ""

        # questions = questions.query(query)
        try:
            self.q_dict = self.questions.set_index('name').to_dict(orient='index')
        except ValueError:
            logger.error("questions are not unique")
            counter = Counter(self.questions['name'].tolist())
            logger.error(["{}({})".format(i, counter[i]) for i in counter if counter[i] > 1])
            exit(0)
        self.available_questions_per_year = {}

    def was_selected(self, email, year):
        selected = np.nan
        selection_col = "selection_{}".format(year)
        # try all indices
        for y in self.years:
            try:
                sel = self.selection.set_index("email_{}".format(y))

                # debug
                # print(sel)

                selected = True if sel.loc[email][selection_col] == 1 else False
                break
            except KeyError:
                pass
                # logger.warning("could not find {} in {}".format(email, y))

        return selected

    def select_by_history(self, df):
        return df.apply(lambda x: self.was_selected(x['email'], x['year']), axis=1)

    # Map observations to desired values, using the corresponding
    # question's scale definition
    def get_q_map(self, q):
        q_scale = self.q_dict[q]['scale']
        q_map = {}

        # if question is not asscoiated with any scale, no mapping is necessary
        if q_scale in self.scales.groups.keys():
            for r in self.scales.get_group(q_scale).iterrows():
                r = r[1]
                for n in range(1, 50):
                    key_name = 'alternative_{}'.format(n)
                    if key_name in r.keys():
                        original = r['alternative_{}'.format(n)]
                        if not pd.isnull(original):
                            q_map[original] = r['value']

        return q_map

    # return id of associated panel entity based on person id (email) and year of observation
    def identify(self, email, year):
        panel_entity_id = None
        person_id = None
        try:
            person = self.persons.query(
                'email=="{}" and wave_added<={}'.format(email, year)
            ).sort_values('wave_added', ascending=False).iloc[0]

            # debug
            # print(person)

            panel_entity_id = person['panel_entity_id']
            person_id = person['id']

            # panel_entity = self.panel_entities.query(
            #    'panel_entity_id=="{}"'.format(panel_entity_id)
            #    ).iloc[0]
            # panel_entity = self.panel_entities.query(
            #    'id=={}'.format(panel_entity_id)
            #    ).iloc[0]

            # company = self.companies.query(
            #    'id=={}'.format(panel_entity['company_id'])
            #    ).iloc[0]
            # din_id = company['din_kundennummer']
        except IndexError as e:

            # debug
            # print(email)
            # print(year)
            # print(panel_entity_id)
            # print(self.panel_entities)
            # print(self.panel_entities.query('id==0'))
            # print(self.panel_entities.query('id==0').iloc[0])

            # raise e

            logger.warn('Could not identify {} in {}'.format(email, year))
            logger.warn('Add them to sheet {} in {}'.format(
                self.settings['sheet_persons'], self.settings['path_panel']))

        return {
            'panel_entity_id': panel_entity_id,
            'person_id': person_id
        }

    # Algorithm for selecting DNP participants
    # ========================================
    #
    # When there are multiple participants who answer for the same company in
    # one wave, a set of rules has to determine the one participant whose
    # answers to take into account.
    #
    # Prerequisites: All participants need to be matched to a company.
    #                Company names have to be cleaned / coded, considering a
    #                threshold up to which organization subdivisions are
    #                regarded as separate units.
    #
    # Input: A set of participants, where each participant represents the same
    #        company. Required information (df columns) per participant:
    #
    # •    fill [float]          Percentage of questions answered (“Füllgrad”)
    # •    position [bool]      The participant’s stated main activity / position
    #                         is standardization
    # •    employees [int]          The associated company’s number of employees, as
    #                         stated by participant
    # •    turnover [int]          The associated company’s turnover, as stated by
    #                         participant
    # •    missing_sector [bool] True if participant has not stated the
    #                         associated company’s sector
    # •    answered [bool]          fill=100%
    #
    # Output: A pandas Series that indicates which participant has been selected
    #         “selected [bool]”.
    #
    @staticmethod
    def _select(group):
        selected = []
        # order by fraction of answered questions (most first)
        ranked = group.sort_values(['fill'], ascending=False)

        has_view = not pd.isnull(ranked['view'].iloc[0])
        above_threshold = max(ranked['fill']) >= 0.1

        if len(group) == 1:
            selected = [2] if has_view and above_threshold else [0]
        else:
            ranked['fill_rank'] = pd.Series(
                range(1, len(ranked) + 1), index=ranked.index)
            max_fill = max(ranked['fill'])
            ranked['dfill'] = ranked['fill'].apply(lambda x: max_fill - x)

            # iterate through ranks
            for i, r in ranked.iterrows():

                if sum(selected) == 0 and r['std_position'] and r['dfill'] <= 0.1:
                    selected.append(1)
                else:
                    selected.append(0)

        if sum(selected) == 0 and has_view and above_threshold:
            selected[0] = 1

        return pd.Series(selected, index=ranked.index)

    @staticmethod
    def select(panel):

        criteria = None
        try:
            criteria = pd.DataFrame.from_dict({
                'panel_entity_id': panel['panel_entity_id'],
                'person_id': panel['person_id'],
                'year': panel['year'],
                'fill': panel['total_fill'],
                'std_position': panel['cat_position'].str.contains('standard', case=False),
                'num_empl': panel['num_empl'],
                'num_turnover': panel['num_turnover'],
                'view': panel['view']
            })
        except Exception as e:
            logger.error("Data is missing for {}:".format(panel['year'].unique()[0]))
            logger.error(e)
            logger.info("Make sure that the following variables exist and have completely mapped scales:")
            logger.info("email, cat_position, num_empl, num_turnover, view")
            exit(0)

        selection = []
        for name, group in criteria.groupby(['panel_entity_id', 'year']):
            group_selection = DataSetCreator._select(group)
            selection.append(group_selection)

        selected = pd.concat(selection)

        return selected > 0, selected.replace({
            0: 'other representataive selected',
            1: 'selected as representative',
            2: 'only representative'
        })

    def make_dataset(self, obs_df, selected_years):

        # keep only observations for selected years
        df = obs_df[obs_df['year'].isin(selected_years)].dropna(axis=1, how='all')

        # calculate fill for one or multiple years
        df['total_fill'] = df['person_id'].map(
            df.groupby(['person_id', 'year']).agg('count').groupby(
                'person_id').sum().sum(axis=1).divide(len(df.columns) * len(self.years))
        )

        # create "selection" column
        logger.info("Selecting observations for panel...")
        selection, selection_reason = DataSetCreator.select(df)
        df['algorithmic_selection'] = selection
        df['algorithmic_selection_comment'] = selection_reason

        # create "historic selection" column
        logger.info("Copying historic selection...")
        df['historic_selection'] = self.select_by_history(df)

        # create merged selection column (history if available, otherwise alg. selection)
        df['selected'] = df['historic_selection'].fillna(df['algorithmic_selection'])

        # select the questions to be dropped
        # include special sections in the "single year" sheets...
        query = self.single_query if len(selected_years) == 1 else self.panel_query
        try:
            all_cols = set(df.columns)
            self.questions['deprecated'] = self.questions['deprecated'].astype('bool')
            relevant_questions = set(self.questions.query(query, engine='python')['name'].tolist())
            extra_cols = set([
                'selected', 'algorithmic_selection', 'algorithmic_selection_comment',
                'historic_selection', 'panel_entity_id', 'person_id', 'year'
            ])

            drop_cols = all_cols - relevant_questions - extra_cols
        except Exception as e:
            logger.warn("Problems with the question definitions in the panel file")
            raise e
        df = df.drop(drop_cols, axis=1)

        return df

    def map_scales(self):
        obs_map = {}

        for y in self.years:
            for q in self.q_dict.keys():
                obs_map[self.q_dict[q]['name_{}'.format(y)]] = self.get_q_map(q)

        for k, d in self.data.items():
            for c in d.columns:
                if c in obs_map.keys():
                    col = d[c].astype(str).replace(obs_map[c], inplace=False)
                    if c in self.q_dict.keys() and self.q_dict[c]['format'] == 'numeric':
                        col = pd.to_numeric(col, errors='coerce')
                    d[c] = col

    def make_panel_data(self):
        self.panel_data = []

        # print(self.data)

        # iterate over columns (questions)
        for k, d in self.data.items():
            qs = {q_info['name_{}'.format(k)]: q_panel for q_panel, q_info in self.q_dict.items()}
            found_qs = [k for k in qs.keys() if k in d.columns]
            not_found_qs = list(set(qs.keys()) - set(found_qs))
            self.available_questions_per_year[k] = [qs[f] for f in found_qs]
            for _ in d.iterrows():
                row = _[1]
                self.panel_data.append({
                    **{'year': k},
                    **{qs[q]: row[q] for q in found_qs},
                    **{qs[nf_q]: np.nan for nf_q in not_found_qs}
                })

    def identify_entities(self):
        self.panel_df = pd.DataFrame(self.panel_data).replace({'nan': np.nan})
        ident = self.panel_df.apply(
            lambda x: self.identify(x['email'], x['year']), axis=1).apply(pd.Series)

        self.panel_df['panel_entity_id'] = ident['panel_entity_id']
        self.panel_df['person_id'] = ident['person_id']
        # self.panel_df['panel_entity_id'] = self.panel_df.apply(
        #    lambda x: self.identify(x['email'], x['year'])[0], axis=1)
        # self.panel_df['person_id'] = self.panel_df.apply(
        #    lambda x: self.identify(x['email'], x['year'])[1], axis=1)

    def prepare_datasets_for_excel(self):
        self.datasets = {}
        if len(self.years) == 1:
            self.datasets = {
                '{}'.format(self.years[0]): self.make_dataset(self.panel_df, self.years)
            }
        elif len(self.years) > 1:
            self.datasets = {
                'panel {}-{}'.format(self.years[0], self.years[-1]): self.make_dataset(self.panel_df, self.years),
                **{'{}'.format(y): self.make_dataset(self.panel_df, [y]) for y in self.years}
            }

    def save(self):
        fn = self.settings['path_output'].format(min(self.years), max(self.years))
        filename = '{}_{}.xlsx'.format(fn, time.strftime("%Y%m%d-%H%M%S"))

        writer = pd.ExcelWriter(filename, engine='xlsxwriter')

        info_years = "the years {} to {}".format(
            min(self.years), max(self.years)) if len(self.years) > 1 else "{}".format(self.years[0])

        info = [
            "German Standardization Panel",
            "Deutsches Normungspanel (DNP)",
            "",
            "This data set contains pseudonymized survey data from " + info_years + ".",
            "It was created automatically on " + time.strftime("%Y-%m-%d") + ".",
            "The data structure is described in the sheets 'variables' and 'scales'.",
            "Observations:"
        ]

        for n, dat in self.datasets.items():
            c_text = "Sheet {}: Selected observations: {}. Total: {}.".format(
                n,
                len(dat[dat['selected'] == True].index),
                len(dat.index)
            )
            if "-" in n:
                c_text += " Panel observations: "
                selected = dat[dat['selected'] == True]
                try:
                    selected_grouped = selected.groupby('panel_entity_id').agg({'year': pd.Series.nunique})
                    yearly = selected_grouped['year'].value_counts().to_dict()
                    c_text += ". ".join(["{} year(s): {}".format(y, c) for y, c in yearly.items()])
                except KeyError:
                    logger.error(selected.groupby('panel_entity_id'))

            info.append(c_text)

        pd.DataFrame({'info': info}).to_excel(writer, sheet_name='info')

        # write questions and scales
        questions_drop_cols = set(self.questions.columns) - set(
            ['name', 'question', 'label', 'scale', 'format']
        )
        self.questions.drop(questions_drop_cols, axis=1).to_excel(writer, sheet_name='variables')

        scales_drop_cols = set(self.scales.obj.columns) - set(
            ['scale', 'value']
        )
        self.scales.obj.drop(scales_drop_cols, axis=1).to_excel(writer, sheet_name='scales')

        # add sheet with data for each year + panel
        for n, dat in self.datasets.items():
            dat.to_excel(writer, sheet_name=n)

        # stack all years and put them in one sheet
        years_dat = [d for y, d in self.datasets.items() if y.isdigit()]
        pd.concat(years_dat, sort=False).to_excel(writer, sheet_name="data")

        writer.save()

        return filename

    def run(self):

        logger.info("Loading files...")
        self.load_files()

        logger.info("Preparing questions...")
        self.prepare_questions()

        logger.info("Mapping scales...")
        self.map_scales()

        logger.info("Creating long format...")
        self.make_panel_data()

        logger.info("Identifying panel entities...")
        self.identify_entities()

        logger.info("Converting to excel...")
        self.prepare_datasets_for_excel()

        logger.info("Saving...")
        filename = self.save()

        logger.info("Dataset saved to {}".format(filename))


if __name__ == "__main__":
    settings = DEFAULT_SETTINGS
    try:
        with open('settings/create_dataset_settings.json') as handle:
            minified = jsmin(handle.read())
            settings = json.loads(minified)
        logger.info("Loaded custom settings from create_dataset_settings.json")
    except Exception as e:
        logger.error(e)
        logger.info("Could not load custom settings, using default settings")
    DataSetCreator(settings=settings).run()