from __future__ import annotations

from collections.abc import Mapping, Sequence
import logging
import traceback

import pandera.polars as pa
import polars as pl

from peh_validation_library.config.config_reader import ConfigReader
from peh_validation_library.core.models.schemas import (
    DFSchema,
)
from peh_validation_library.core.utils.mappers import validation_type_mapper
from peh_validation_library.dataframe.df_reader import read_dataframe
from peh_validation_library.error_report.error_collector import (
    ErrorCollector,
)
from peh_validation_library.error_report.error_schemas import (
    ExceptionSchema,
)

logger = logging.getLogger(__name__)


class Validator:
    """Validator class for validating a DataFrame given config schema.

    Attributes:
        dataframe (pl.DataFrame): The DataFrame to be validated.
        config (DFSchema): The schema configuration for validation.
        logger (logging.Logger): Logger instance for logging validation steps.

    """

    def __init__(
        self,
        dataframe: pl.DataFrame,
        config: DFSchema,
        logger: logging.Logger = logger,
    ) -> None:
        self.dataframe = dataframe
        self.config = config
        self.__logger = logger
        self.__error_collector = ErrorCollector()

    def validate(self) -> None:
        """Validates the DataFrame against the schema defined in the config.

        Returns:
            None

        """

        self.__logger.info(f'Building DataFrame schema {self.config.name =}')
        df_schema = self.config.build()

        try:
            self.__logger.info('Casting DataFrame Types')
            self.dataframe = self.dataframe.cast({
                col.id: validation_type_mapper[col.data_type]
                for col in self.config.columns
                if col.id in self.dataframe.columns
            })
            self.__logger.info('Starting DataFrame validation')
            self.dataframe.pipe(df_schema.validate, lazy=True)

        except pa.errors.SchemaErrors as err:
            self.__logger.info('Collecting validation errors')
            self.__error_collector.add_errors(err)
        # Pandera not implemented for polars some lazy validation.
        # Run in again in eager mode to catch the error.
        # This is a workaround for the issue.
        except NotImplementedError:
            try:
                self.__logger.warning('Trying eager validation')
                self.dataframe.pipe(df_schema.validate)

            except pa.errors.SchemaError as err:
                self.__logger.warning('Collecting eager validation error')
                self.__error_collector.add_errors(err)
        except Exception as err:
            msg = f'Error validating dataframe: {err}'
            self.__logger.error(msg)
            error_traceback = traceback.format_exc()
            self.__error_collector.add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.validate',
                    error_source=__name__,
                )
            )

    @classmethod
    def build_validator(
        cls,
        config: Mapping[str, str | Sequence | Mapping],
        dataframe: dict[str, Sequence],
        logger: logging.Logger = logger,
    ) -> Validator:
        """Builds a Validator instance from a configuration and a DataFrame.

        Examples:
            The command is either a user-defined function or a string that
            maps to a function that will be used to validate the DataFrame.

            The following commands are available:

                'is_equal_to',
                'is_equal_to_or_both_missing',
                'is_greater_than_or_equal_to',
                'is_greater_than',
                'is_less_than_or_equal_to',
                'is_less_than',
                'is_not_equal_to',
                'is_not_equal_to_and_not_both_missing',
                'is_unique',
                'is_duplicated',
                'is_in',
                'is_null',
                'is_not_null'

            >>> config_input = {
                    "name": "example_schema",
                    "columns": [
                        {
                            "id": "column1",
                            "data_type": "integer",
                            "nullable": False,
                            "unique": True,
                            "required": True,
                            "checks": [
                                {
                                    "command": "is_equal_to",
                                    "subject": ["column2"]
                                }
                            ]
                        },
                        {
                            "id": "column2",
                            "data_type": "varchar",
                            "nullable": True,
                            "unique": False,
                            "required": False,
                            "checks": [
                                {
                                    'check_case': 'condition',
                                    'expressions': [
                                        {
                                            'command': 'is_in',
                                            'arg_values': ['a', 'b', 'c']
                                        },
                                        {
                                            'command': 'is_equal_to',
                                            'subject': ['column1'],
                                            'arg_values': [1]
                                        }
                                    ]
                                }
                            ],
                        }
                    "ids": ["column1"],
                    "metadata": {"description": "Example DataFrame schema"},
                    "checks": [
                        {
                            'name': 'example_check',
                            'error_level': 'warning',
                            'error_msg': 'This is an example check',
                            'command': 'is_in',
                            'subject': ['column1', 'column2'],
                            'arg_values': [1, 2]
                        }
                    ]
                }
            >>> dataframe_input = {
                    "column1": [1, 2, 3],
                    "column2": ["a", "b", "c"]
                }


        Args:
            config (Mapping[str, str | Sequence | Mapping]): Configuration
                for the DataFrame schema.
            dataframe (dict[str, Sequence]): DataFrame data to be validated.
            logger (logging.Logger, optional): Logger instance for logging.
                Defaults to the module logger.

        Returns:
            Validator: An instance of the Validator class.

        """
        try:
            df = read_dataframe(dataframe)
            config_reader = ConfigReader(config)
            df_schema = config_reader.get_df_schema()
        except Exception as err:
            msg = f'Error reading inputs: {err}'
            logger.error(msg)
            error_traceback = traceback.format_exc()
            ErrorCollector().add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.build_validator',
                    error_source=__name__,
                )
            )
            return ErrorCollector().get_errors()

        logger.info('Validator build complete')

        return cls(dataframe=df, config=df_schema, logger=logger)
