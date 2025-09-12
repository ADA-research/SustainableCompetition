import polars as pl


class AbstractPerformanceModel:
    """A performance model learning from 4 input files (currently, corresponding to the 4 tables in our future database)
    """

    def __init__(self):
        """Initialises the model

        Args:
            should take the RF parameters

        """
        pass

    def train(self, X, Y):
        """train the model
        for now very basic but could be better tailored in the future (handling saturated right tail for example)
        """
        pass

    def predict(self, x_input):
        """predicts stuff, for now simple but could be refined

        Args:
            x_input (dataframe): _description_

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """

        pass
