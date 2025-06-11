from __future__ import annotations

from collections.abc import Mapping, Sequence
import logging
import traceback

import pandera.polars as pa
import polars as pl

from peh_validation_library.config.config_reader import get_df_schema
from peh_validation_library.core.utils.mappers import validation_type_mapper
from peh_validation_library.dataframe.df_reader import read_dataframe
from peh_validation_library.error_report.error_collector import (
    ErrorCollector,
)
from peh_validation_library.error_report.error_schemas import (
    ExceptionSchema,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Validator:
    """Validator class for validating DataFrames against a defined schema."""

    error_collector = ErrorCollector()

    @classmethod
    def config_from_mapping(
        cls,
        config: Mapping[str, str | Sequence | Mapping],
        collect_exceptions: bool = True,
        logger: logging.Logger = logger,
    ) -> Validator:
        """Creates a DFSchema from a configuration mapping.

        Args:
            config (Mapping[str, str | Sequence | Mapping]): Configuration
                mapping for the DataFrame schema.
            collect_exceptions (bool, optional): Whether to collect exceptions
                during the schema creation. Defaults to True.
            logger (logging.Logger, optional): Logger instance for logging.
                Defaults to the module logger.

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

        Returns:
            Validator: An instance of the Validator class with the schema
                created from the provided configuration mapping.

        Raises:
            Exception: If an error occurs while reading the configuration
                or creating the schema, and collect_exceptions is False.

        """
        validator = cls()
        try:
            validator.df_schema = get_df_schema(config)

        except Exception as err:
            msg = f'Error reading inputs: {err}'
            logger.error(msg)

            if not collect_exceptions:
                raise err

            error_traceback = traceback.format_exc()
            validator.error_collector.add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.config_from_mapping',
                    error_source=__name__,
                )
            )

        logger.info('DFSchema created successfully')
        return validator

    def convert_mapping_to_dataframe(
        self,
        dataframe: Mapping[str, list] | pl.DataFrame,
        collect_exceptions: bool = True,
        logger: logging.Logger = logger,
    ) -> pl.DataFrame:
        logger.info('Reading DataFrame from mapping')
        try:
            return read_dataframe(dataframe)
        except Exception as err:
            msg = f'Error converting dataframe: {err}'
            logger.error(msg)

            if not collect_exceptions:
                raise err

            error_traceback = traceback.format_exc()
            self.error_collector.add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.convert_mapping_to_dataframe',
                    error_source=__name__,
                )
            )

    def validate(
        self,
        dataframe: Mapping[str, list] | pl.DataFrame,
        lazy_validation: bool = True,
        collect_exceptions: bool = True,
        logger: logging.Logger = logger,
    ) -> None:
        """Validates a DataFrame against the defined schema.

        Args:
            dataframe (Mapping[str, list] | pl.DataFrame): The input data
                as a mapping or a Polars DataFrame.
            lazy_validation (bool, optional): Whether to perform lazy validation.
                Defaults to True.
            collect_exceptions (bool, optional): Whether to collect exceptions
                during validation. Defaults to True.
            logger (logging.Logger, optional): Logger instance for logging.
                Defaults to the module logger.

        Raises:
            Exception: If an error occurs during validation and
                collect_exceptions is False.

        """  # noqa: E501
        logger.info('Starting DataFrame validation')
        if isinstance(dataframe, Mapping):
            dataframe = self.convert_mapping_to_dataframe(
                dataframe=dataframe,
                collect_exceptions=collect_exceptions,
                logger=logger,
            )

        try:
            logger.info(f'Building DataFrame schema {self.df_schema.name =}')
            df_schema = self.df_schema.build()
        except Exception as err:
            msg = f'Error building dataframe schema: {err}'
            logger.error(msg)

            if not collect_exceptions:
                raise err

            error_traceback = traceback.format_exc()
            self.error_collector.add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.validate',
                    error_source=__name__,
                )
            )
            return

        try:
            logger.info('Casting DataFrame Types')
            dataframe = dataframe.cast({
                col.id: validation_type_mapper[col.data_type]
                for col in self.df_schema.columns
                if col.id in dataframe.columns
            })

        except Exception as err:
            msg = f'Error casting dataframe types: {err}'
            logger.error(msg)

            if not collect_exceptions:
                raise err

            error_traceback = traceback.format_exc()
            self.error_collector.add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.validate',
                    error_source=__name__,
                )
            )
            return

        try:
            logger.info('Starting DataFrame validation')
            dataframe.pipe(df_schema.validate, lazy=lazy_validation)

        except (pa.errors.SchemaErrors, pa.errors.SchemaError) as err:
            if not collect_exceptions:
                raise err

            logger.info('Collecting validation errors')
            self.error_collector.add_errors(err)

        # Pandera not implemented for polars some lazy validation.
        # Run in again in eager mode to catch the error.
        # This is a workaround for the issue.
        except NotImplementedError:
            try:
                logger.warning('Trying eager validation')
                dataframe.pipe(df_schema.validate)

            except pa.errors.SchemaError as err:
                if not collect_exceptions:
                    raise err

                logger.info('Collecting eager validation errors')
                self.error_collector.add_errors(err)

        except Exception as err:
            msg = f'Error validating dataframe: {err}'
            logger.error(msg)

            if not collect_exceptions:
                raise err

            error_traceback = traceback.format_exc()
            self.error_collector.add_errors(
                ExceptionSchema(
                    error_type=type(err).__name__,
                    error_message=str(err),
                    error_level='critical',
                    error_traceback=error_traceback,
                    error_context='Validator.validate',
                    error_source=__name__,
                )
            )

        logger.info('DataFrame validation completed')
