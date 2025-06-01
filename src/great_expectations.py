import great_expectations as gx

def test_expectation(df):
    # Retrieve your Data Context
    context = gx.get_context()

    data_source_name = "my_data_source"
    data_asset_name = "my_dataframe_data_asset"
    batch_definition_name = "my_batch_definition"
    batch_definition = (
            context.data_sources.add_pandas(data_source_name)  
            .add_dataframe_asset(data_asset_name)
            .add_batch_definition_whole_dataframe(batch_definition_name)
        )
    
    batch_parameters = {"dataframe": df}

    expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column="age", max_value=120, min_value=0
        )

    # Get the dataframe as a Batch
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(expectation)

    return validation_results


