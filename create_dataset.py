import pandas as pd
import numpy as np
import time
import sys
import json
from jsmin import jsmin
from collections import Counter
import os.path
from xlrd.biffh import XLRDError
from pprint import pprint

# set up logging (to console)
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

PATH_TO_SETTINGS = 'settings/settings.json'

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
    path_output='output/dnp_panel-data_{}-{}',

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


def load_excel_sheet(file, sheet=None, exit_on_error=True):
    """
    Load an Excel sheet and return as Pandas dataframe with user friendly error-handling.

    :param file: Path to excel file
    :param sheet: Name of sheet to load. Default: None -> Loads first sheet.
    :param exit_on_error: Whether to call exit(0) on exception.
    :returns: Pandas DataFrame, or None in case of error.
    """
    df = None
    try:
        if sheet is not None:
            df = pd.read_excel(file, sheet_name=sheet)
        else:
            df = pd.read_excel(file)
    except FileNotFoundError:
        logger.error("Could not find file '{}'.".format(file))
        if exit_on_error:
            exit(0)
    except pd.errors.EmptyDataError:
        logger.error("File '{}' is empty.".format(file))
        if exit_on_error:
            exit(0)
    except pd.errors.ParserError:
        logger.error("File '{}' is broken.".format(file))
        if exit_on_error:
            exit(0)
    except XLRDError:
        logger.error("Sheet '{}' is missing in {}.".format(sheet, file))
        if exit_on_error:
            exit(0)
    except Exception:
        logger.error("There was an error while loading '{}'".format(file))
        if exit_on_error:
            exit(0)

    return df


class DataSetType:
    SINGLE_YEAR = 1
    PANEL = 2


class DataSetCreator(object):

    dataset_extra_cols = {
        'selected', 'algorithmic_selection',
        'algorithmic_selection_comment', 'historic_selection',
        'panel_entity_id', 'person_id', 'year'
    }

    required_settings = ['path_panel', 'sheet_persons', 'sheet_panel_entities',
                         'sheet_companies', 'sheet_groups', 'sheet_questions',
                         'sheet_scales', 'sheet_selection', 'path_results',
                         'path_output', 'years', 'exclude_deprecated',
                         'exclude_personal', 'exclude_auxiliary',
                         'exclude_din_meta_data', 'exclude_special_sections',
                         'exclude_years_min', 'allow_missing', 'save_output',
                         'use_selection_history']

    def __init__(self, settings=DEFAULT_SETTINGS):

        # settings: dataframe
        self.settings = settings
        self.years = self.settings['years']
        self._validate_settings()

        # list people (per year) that can't be found when creating dataset
        self.missing_persons = {y: set() for y in self.years}

        # load data according to settings...
        # data: dictionary year:dataframe per self.years
        # questions, q_dict, scales: data structure dataframes
        # participant database: dataframes
        # selection: historic (who was in the samples of the past?), dataframes
        logger.info("Loading data...")
        self.data, self.questions, self.q_dict, self.scales, self.persons, self.companies,\
        self.panel_entities, self.groups,\
        self.selection = DataSetCreator._load_data(
            settings=self.settings
        )

        logger.info("Mapping scales...")
        self.data = self._map_scales(
            data=self.data,
            scales=self.scales,
            q_dict=self.q_dict
        )

        logger.info("Generating panel data...")
        self.panel_df, self.available_questions_per_year = self._make_panel_df()

        # warn if persons couldn't be identified
        for y, missing in self.missing_persons.items():
            if len(missing) > 0:
                logger.warning("Missing in {}: {}".format(y, ", ".join(missing)))

    def _validate_settings(self):
        for s in self.required_settings:
            if s not in self.settings.keys():
                logger.error("Setting '{}' is missing.".format(s))
                exit(0)

        if not os.path.isfile(self.settings["path_panel"]):
            logger.error("Panel file '{}' could not be found.".format(self.settings["path_panel"]))
            exit(0)

        outdir = "/".join(self.settings["path_output"].split("/")[:-1])
        if not os.path.isdir(outdir):
            logger.error("Can't find output directory '{}'.".format(outdir))
            exit(0)

    def _set_sheet_settings_attribute(self, attr_name):
        """
        Sets an object attribute (=attr_name) to a df generate from the sheet
        of the same name in the panel file ("panel.xlsx").

        :param attr_name: Object attribute = sheet name to set
        :return: None
        """
        setattr(self, attr_name, load_excel_sheet(
            self.settings['path_panel'],
            self.settings['sheet_{}'.format(attr_name)]
        ))

    def _get_relevant_questions(self, dataset_type=DataSetType.SINGLE_YEAR):
        """
        Get a set of question names that should be included in dataset
        according to settings: in/exclude deprecated, personal, auxiliary questions,
        meta data, questions that appear in a minimum number of years, special sections, etc.

        :param dataset_type: default: DataSetType.SINGLE_YEAR. If DataSetType.PANEL, exclude
                             special section questions
        :return: Set of question names
        """
        query =  'name != "NaN"'
        query += ' and not deprecated' if self.settings['exclude_deprecated'] else ""
        query += ' and not personal_data' if self.settings['exclude_personal'] else ""
        query += ' and not auxiliary' if self.settings['exclude_auxiliary'] else ""
        query += ' and not name.str.startswith("din_")' if self.settings['exclude_din_meta_data'] else ""
        query += ' and num_years_observed >= {}'.format(self.settings['exclude_years_min'])

        if dataset_type==DataSetType.PANEL:
            query += ' and special_section_year == "NaN"' if self.settings[
                'exclude_special_sections'] else ""

        return set(self.questions.query(query, engine='python')['name'].tolist())

    @staticmethod
    def _load_data(settings):
        """
        Load survey data files and additional settings from panel.xlsx.

        :param settings:
        :return: DataFrames (data, questions, q_dict, scales, persons,
                             companies, groups, panel_entities, selection)
        """

        # load survey data
        data = {y: load_excel_sheet(
            settings['path_results'].format(y))
            for y in settings['years']
        }

        # load sheets from panel.xlsx
        questions = load_excel_sheet(settings['path_panel'], settings['sheet_questions'])
        scales = load_excel_sheet(settings['path_panel'], settings['sheet_scales'])
        persons = load_excel_sheet(settings['path_panel'], settings['sheet_persons'])
        companies = load_excel_sheet(settings['path_panel'], settings['sheet_companies'])
        groups = load_excel_sheet(settings['path_panel'], settings['sheet_groups'])
        panel_entities = load_excel_sheet(settings['path_panel'], settings['sheet_panel_entities'])
        selection = load_excel_sheet(settings['path_panel'], settings['sheet_selection'])

        # group scale entries
        scales = scales.groupby('scale')

        # make sure deprecated works...
        questions['deprecated'] = questions['deprecated'].astype('bool')

        # save dict representation of questions
        try:
            q_dict = questions.set_index('name').to_dict(orient='index')
        except ValueError:
            logger.error("questions are not unique")
            counter = Counter(questions['name'].tolist())
            logger.error(["{}({})".format(i, counter[i]) for i in counter if counter[i] > 1])
            exit(0)
        except Exception as ex:
            logger.error("Can not generate question dictionary, questions not properly loaded or defined")
            raise ex

        return data, questions, q_dict, scales, persons,\
               companies, groups, panel_entities, selection

    def _was_selected(self, email, year):
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

    def _select_by_history(self, df):
        return df.apply(lambda x: self._was_selected(x['email'], x['year']), axis=1)

    # Map observations to desired values, using the corresponding
    # question's scale definition
    @staticmethod
    def _get_q_map(q, q_dict, scales):
        """
        Map observations to desired values, using the corresponding
        question's scale definition.

        A default mapped value can be set by setting the $default$ flag,
        all original data values that can't be mapped will be mapped
        to the default value.

        :param q: question name
        :param q_dict: question dictionary (q->{...,scale: scalename})
        :param scales: Pandas DataFrame
        :return: dict {original_data_value: mapped_data_value}
        """
        q_scale = q_dict[q]['scale']
        q_map = {}

        # if question is not associated with any scale, no mapping is necessary
        if q_scale in scales.groups.keys():
            for r in scales.get_group(q_scale).iterrows():
                r = r[1]
                for n in range(1, 50):
                    key_name = 'alternative_{}'.format(n)
                    if key_name in r.keys():
                        original = r['alternative_{}'.format(n)]
                        if not pd.isnull(original):
                            # cast all values to str to avoid type mismatches
                            q_map[str(original)] = str(r['value'])

        return q_map

    def identify(self, email, year):
        """
        Get id of associated panel entity and person based on email and year of observation.

        :param email: person's e-mail address
        :param year: year of entry in participant DB
        :return: {panel_entity_id:int value, person_id:int value}
        """
        panel_entity_id = None
        person_id = None
        try:
            person = self.persons.query(
                'email=="{}" and wave_added<={}'.format(email, year)
            ).sort_values('wave_added', ascending=False).iloc[0]

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
        except IndexError:
            # logger.warning('Could not identify {} in {}'.format(email, year))
            # logger.warning('Add them to sheet {} in {}'.format(
            #    self.settings['sheet_persons'], self.settings['path_panel'])
            # )
            self.missing_persons[year].add(email)

        return {
            'panel_entity_id': panel_entity_id,
            'person_id': person_id
        }

    @staticmethod
    def _select(group):
        """
        Algorithm for selecting DNP participants:

        When there are multiple participants who answer for the same company in
        one wave, a set of rules has to determine the one participant whose
        answers to take into account.

        Prerequisites: All participants need to be matched to a company.
                       Company names have to be cleaned / coded, considering a
                       threshold up to which organization subdivisions are
                       regarded as separate units.

        Input:  Required information (df columns) per participant:

        • fill [float]         Percentage of questions answered (“Füllgrad”)
        • position [bool]      The participant’s stated main activity / position
                               is standardization
        • employees [int]      The associated company’s number of employees, as
                               stated by participant
        • turnover [int]       The associated company’s turnover, as stated by
                               participant
        • missing_sector [bool] True if participant has not stated the
                                associated company’s sector
        • answered [bool]       fill=100%
        :param group: A Pandas group of participants, where each participant represents
                      the same company.
        :return: Pandas Series with bool selected
        """
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
        """
        Select one participant from all participant groups in the data.
        Calls DataSetCreator._select on each group. Appends selection reasons.

        :param panel: Pandas DataFrame
        :return:
        """

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
            # debug
            # logger.info("positions: ")
            # logger.info(panel['cat_position'].value_counts())
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
        logger.info("{}: selected {}/{} participants".format(
            ", ".join([str(y) for y in panel['year'].unique().tolist()]),
            len(selected[selected>0].index), len(selected)
        ))

        return selected > 0, selected.replace({
            0: 'other representative selected or not enough questions answered',
            1: 'selected as representative',
            2: 'only representative'
        })

    def make_dataset(self, data, selected_years):
        """
        Prepare a dataset from given data for the specified years:
        - select a sample of participants based on DataSetCreator.select()
        - include historical selection if available
        - drop variables that are not available in time-span

        :param data: Pandas DataFrame (stacked panel data in long format with var 'year')
        :param selected_years: list<int> of years
        :return: dataset (Pandas DataFrame)
        """

        # keep only observations for selected years
        df = data[data['year'].isin(selected_years)].dropna(axis=1, how='all')

        # calculate fill for one or multiple years
        df['total_fill'] = df['person_id'].map(
            df.groupby(['person_id', 'year']).agg('count').groupby(
                'person_id').sum().sum(axis=1).divide(len(df.columns) * len(self.years))
        )

        # create "selection" column
        selection, selection_reason = DataSetCreator.select(df)
        df['algorithmic_selection'] = selection
        df['algorithmic_selection_comment'] = selection_reason

        # create "historic selection" column
        logger.info("Copying historic selection...")
        df['historic_selection'] = self._select_by_history(df)

        # create merged selection column (history if available, otherwise alg. selection)
        # TODO: review
        df['selected'] = df['historic_selection'].fillna(df['algorithmic_selection'])

        # drop questions that are not required
        try:
            relevant_questions = self._get_relevant_questions(
                DataSetType.SINGLE_YEAR if len(selected_years) == 1
                else DataSetType.PANEL
            )
            drop_cols = set(df.columns) - relevant_questions - DataSetCreator.dataset_extra_cols
            df = df.drop(drop_cols, axis=1)
        except Exception as ex:
            logger.error("An error occured when trying to drop irrelevant questions:")
            raise ex
            exit(0)

        return df

    @staticmethod
    def _map_scales(data, scales, q_dict):
        """
        Replaces all data values for questions that have defined scales.

        :param data: dict{year:DataFrame} of survey data
        :param scales:
        :param q_dict:
        :return:
        """
        obs_map = {}

        for y in data.keys():
            for q in q_dict.keys():
                obs_map[q_dict[q]['name_{}'.format(y)]] = DataSetCreator._get_q_map(q, q_dict, scales)

        for y in data.keys():
            for c in data[y].columns:
                if c in obs_map.keys():
                    data[y][c] = data[y][c].astype(str).replace(obs_map[c])
                    if c in q_dict.keys() and q_dict[c]['format'] == 'numeric':
                        data[y][c] = pd.to_numeric(data[y][c], errors='coerce')

        return data

    def _make_panel_df(self):
        """
        Create panel DataFrame by stacking yearly data, adding year variable, and identifying
        persons and panel entities by <year, email> information.

        :return: panel (Pandas DataFrame), number of available questions per year (dict{year:num})
        """

        panel_data = []
        available_questions_per_year = {}

        # iterate over columns (questions)
        for k, d in self.data.items():
            qs = {q_info['name_{}'.format(k)]: q_panel for q_panel, q_info in self.q_dict.items()}
            found_qs = [k for k in qs.keys() if k in d.columns]
            not_found_qs = list(set(qs.keys()) - set(found_qs))
            available_questions_per_year[k] = [qs[f] for f in found_qs]
            for _ in d.iterrows():
                row = _[1]
                panel_data.append({
                    **{'year': k},
                    **{qs[q]: row[q] for q in found_qs},
                    **{qs[nf_q]: np.nan for nf_q in not_found_qs}
                })

            # identify panel entities
            panel_df = pd.DataFrame(panel_data).replace({'nan': np.nan})
            ident = panel_df.apply(
                lambda x: self.identify(x['email'], x['year']), axis=1).apply(pd.Series)

            panel_df['panel_entity_id'] = ident['panel_entity_id']
            panel_df['person_id'] = ident['person_id']

        return panel_df, available_questions_per_year

    def get_datasets(self, years=None):
        if years is None:
            years = self.years
        datasets = {}
        if len(years) == 1:
            datasets = {
                '{}'.format(years[0]): self.make_dataset(self.panel_df, years)
            }
        elif len(years) > 1:
            datasets = {
                'panel {}-{}'.format(years[0], years[-1]): self.make_dataset(self.panel_df, years),
                **{'{}'.format(y): self.make_dataset(self.panel_df, [y]) for y in years}
            }

        return datasets

    def to_excel(self):
        # prepare datasets
        logger.info("Preparing all datasets...")
        datasets = self.get_datasets()

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

        for n, dat in datasets.items():
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


        # write questions and scales
        questions_drop_cols = set(self.questions.columns) - {
            'name', 'question', 'label', 'scale', 'format'
        }
        scales_drop_cols = set(self.scales.obj.columns) - {'scale', 'value'}

        # sheet 'info'
        pd.DataFrame({'info': info}).to_excel(writer, sheet_name='info')

        # sheet 'variables'
        self.questions.drop(questions_drop_cols, axis=1).to_excel(writer, sheet_name='variables')

        # sheet 'scales'
        self.scales.obj.drop(scales_drop_cols, axis=1).to_excel(writer, sheet_name='scales')

        # sheets yyyy
        for n, dat in datasets.items():
            dat.to_excel(writer, sheet_name=n)

        # stack all years and put them in one sheet
        # TODO: review --> Is this the right method to have the correct selection for complete panel?
        years_dat = [d for y, d in datasets.items() if y.isdigit()]

        # sheet 'data'
        pd.concat(years_dat, sort=False).to_excel(writer, sheet_name="data")

        logger.info("Saving to excel...")
        try:
            writer.save()
        except FileNotFoundError:
            logger.error("Could not save to {}".format(filename))
            exit(0)

        return filename


if __name__ == "__main__":
    settings = DEFAULT_SETTINGS
    try:
        with open(PATH_TO_SETTINGS) as handle:
            minified = jsmin(handle.read())
            settings = json.loads(minified)
        logger.info("Loaded custom settings from {}".format(PATH_TO_SETTINGS))
    except Exception as e:
        logger.error(e)
        logger.info("Could not load custom settings, using default settings")

    creator = DataSetCreator(settings=settings)
    filename = creator.to_excel()
    logger.info("Dataset saved to {}".format(filename))