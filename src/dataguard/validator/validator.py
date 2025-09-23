from __future__ import annotations

from collections.abc import Mapping, Sequence
import logging
import warnings

import pandera.polars as pa
import polars as pl
from pydantic import ValidationError

from dataguard.config.config_reader import get_df_schema
from dataguard.core.utils.mappers import validation_type_mapper
from dataguard.dataframe.df_reader import read_dataframe
from dataguard.error_report.error_collector import (
    ErrorCollector,
)
from dataguard.error_report.handlers import (
    error_handler,
    exception_handler,
    pandera_schema_errors_handler,
)

warnings.filterwarnings(
    'ignore',
    category=UserWarning,
    module='pandera',
    message='unique_column_names=True will have no effect on validation since polars DataFrames do not support duplicate column names.',  # noqa: E501
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
        """Creates a Validator instance from a configuration mapping.

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
                    "ids": ["column1"],
                    "metadata": {"description": "Example DataFrame schema"},
                    "checks": [
                        {
                            'name': 'example_check',
                            'error_level': 'warning',
                            'error_msg': 'This is an example check',
                            'command': 'is_in',
                            'subject': ['column1'],
                            'arg_values': [1, 2]
                        }
                    ]
                }

        Returns:
            Validator: An instance of the Validator class with the schema
                created from the provided configuration mapping.

        """
        validator = cls()
        try:
            validator.df_schema = get_df_schema(config)
            logger.info('DFSchema created successfully')

        except KeyError as err:
            error_handler(
                err=err,
                err_level='critical',
                message=f'Missing the following key in config input: {err.args[0]}',  # noqa: E501
                lazy=collect_exceptions,
                logger=logger,
            )

        except (AttributeError, TypeError) as err:
            error_handler(
                err=err,
                err_level='critical',
                message=f'Invalid config type: {err.args[0]}',
                lazy=collect_exceptions,
                logger=logger,
            )

        except ValidationError as err:
            error_handler(
                err=err,
                err_level='critical',
                message=f'Invalid config type: {[error["loc"] for error in err.errors()]}',  # noqa: E501
                lazy=collect_exceptions,
                logger=logger,
            )

        except Exception as err:
            exception_handler(
                err=err,
                err_level='critical',
                lazy=collect_exceptions,
                logger=logger,
            )

            logger.error('Failed to create DFSchema from configuration')

        return validator

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
        try:
            if not getattr(self, 'df_schema', None):
                logger.error('DataFrame schema is not defined')
                return

            logger.info('Starting DataFrame validation')
            if isinstance(dataframe, Mapping):
                dataframe = convert_mapping_to_dataframe(
                    dataframe=dataframe,
                    collect_exceptions=collect_exceptions,
                    logger=logger,
                )

            if not getattr(dataframe, 'shape', None):
                logger.error('DataFrame is not valid')
                return

            logger.info(f'Building DataFrame schema {self.df_schema.name =}')
            df_schema = self.df_schema.build()

            try:
                logger.info('Casting DataFrame Types')
                dataframe = dataframe.cast({
                    col.id: validation_type_mapper[col.data_type]
                    for col in self.df_schema.columns
                    if col.id in dataframe.columns
                })

                logger.info('Starting DataFrame validation')
                dataframe.pipe(df_schema.validate, lazy=lazy_validation)

            except pl.exceptions.PolarsError as err:
                error_handler(
                    err=err,
                    err_level='critical',
                    message=str(err),
                    lazy=collect_exceptions,
                    logger=logger,
                )

            except (pa.errors.SchemaErrors, pa.errors.SchemaError) as err:
                pandera_schema_errors_handler(
                    err=err,
                    lazy=collect_exceptions,
                    logger=logger,
                )
                logger.info('Collecting validation errors')
            # Pandera not implemented for polars some lazy validation.
            # Run in again in eager mode to catch the error.
            # This is a workaround for the issue.
            except NotImplementedError:
                try:
                    logger.warning('Trying eager validation')
                    dataframe.pipe(df_schema.validate)

                except pl.exceptions.PolarsError as err:
                    error_handler(
                        err=err,
                        err_level='critical',
                        message=str(err),
                        lazy=collect_exceptions,
                        logger=logger,
                    )

                except pa.errors.SchemaError as err:
                    pandera_schema_errors_handler(
                        err=err,
                        lazy=collect_exceptions,
                        logger=logger,
                    )

                logger.info('Collecting eager validation errors')

        except Exception as err:
            exception_handler(
                err=err,
                err_level='critical',
                lazy=collect_exceptions,
                logger=logger,
            )

        logger.info('DataFrame validation completed')


def convert_mapping_to_dataframe(
    dataframe: Mapping[str, list] | pl.DataFrame,
    collect_exceptions: bool = True,
    logger: logging.Logger = logger,
) -> pl.DataFrame | None:
    logger.info('Reading DataFrame from mapping')
    try:
        return read_dataframe(dataframe)

    except pl.exceptions.PolarsError as err:
        error_handler(
            err=err,
            err_level='critical',
            message=str(err),
            lazy=collect_exceptions,
            logger=logger,
        )
        return

    except Exception as err:
        exception_handler(
            err=err,
            err_level='critical',
            lazy=collect_exceptions,
            logger=logger,
        )
        return
