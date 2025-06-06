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
