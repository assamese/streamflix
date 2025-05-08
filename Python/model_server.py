import pandas as pd
import joblib
from sklearn.impute import SimpleImputer
import warnings
import psycopg2
import os

def load_fresh_data(conn, query):
    """
    Loads fresh data from the database into a Pandas DataFrame.

    Args:
        conn: psycopg2 connection object.
        query: SQL query to execute.

    Returns:
        pandas.DataFrame: DataFrame containing the data, or None on error.
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        print(f"Error loading data: {e}")
        return None

def preprocess_fresh_data(df, training_columns):
    """
    Preprocesses the fresh data to be compatible with the trained model.
    This includes one-hot encoding categorical variables, imputing missing values,
    and ensuring the columns match the training data.

    Args:
        df (pandas.DataFrame): DataFrame containing the fresh data.
        training_columns (list): List of columns used during training.

    Returns:
        pandas.DataFrame: Preprocessed DataFrame, or None on error.
    """
    try:
        # Drop columns that are not needed for prediction
        df = df.drop(['event_id', 'event_ts', 'is_completion', 'user_genre_preference'], axis=1, errors='ignore')

        # Separate numeric and non-numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        non_numeric_cols = df.select_dtypes(exclude=['int64', 'float64']).columns

        # Convert categorical variables to dummy variables, handling unknown categories
        df = pd.get_dummies(df, columns=non_numeric_cols, dummy_na=False)

        # Convert boolean columns to numeric
        bool_cols = df.select_dtypes(include=['bool']).columns
        df[bool_cols] = df[bool_cols].astype(int)

        # Impute missing values
        imputer = SimpleImputer(strategy='constant', fill_value=0)
        df = pd.DataFrame(imputer.fit_transform(df), columns=df.columns)

        # Ensure the columns match the training data
        # Add missing columns from training data
        missing_cols = set(training_columns) - set(df.columns)
        if missing_cols:
            # Create a temporary DataFrame with the missing columns
            missing_data = {}
            for c in missing_cols:
                missing_data[c] = [0] * len(df)

            missing_df = pd.DataFrame(missing_data)
            # Concatenate the temporary DataFrame with the original DataFrame
            df = pd.concat([df, missing_df], axis=1)



        # Remove columns not in training data
        extra_cols = set(df.columns) - set(training_columns)
        if extra_cols:
            df = df.drop(list(extra_cols), axis=1)

        # Reorder the columns to match the training data
        df = df[training_columns]

        # Drop rows with NaN values
        df = df.dropna()

        return df
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        return None

def predict_completion(model, df):
    """
    Predicts content completion using the trained model.

    Args:
        model: Trained Logistic Regression model.
        df (pandas.DataFrame): DataFrame containing the preprocessed data.

    Returns:
        pandas.Series: Predicted probabilities of completion, or None on error.
    """
    try:
        # Predict the probability of completion
        y_pred_proba = model.predict_proba(df)[:, 1]
        return y_pred_proba
    except Exception as e:
        print(f"Error making predictions: {e}")
        return None

def main():
    """
    Main function to load the model, load fresh data, preprocess it,
    make predictions, and print the results.
    """
    # Load the trained model
    model_filename = 'completion_model.joblib'  #  Make sure this is the correct path
    try:
        model = joblib.load(model_filename)
        print(f"Loaded trained model from {model_filename}")
    except FileNotFoundError:
        print(f"Error: Model file '{model_filename}' not found.  Make sure the model is saved.")
        return
    except Exception as e:
        print(f"Error loading the model: {e}")
        return

    # Database connection details
    db_host = 'pg-20c7c62-llmtravel.i.aivencloud.com'
    db_port = 21732
    db_name = 'defaultdb'
    db_user = 'avnadmin'
    db_password = os.environ.get('POSTGRES_PASSWORD')
    db_sslmode = 'require'

    conn = None
    try:
        if db_password is None:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable not set.")
        db_connection_string = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
        conn = psycopg2.connect(db_connection_string)

        # Load fresh data from the database
        query = """SELECT * FROM enriched_table"""  #  Change the query as needed
        fresh_df = load_fresh_data(conn, query)
        if fresh_df is None:
            return

        # Get the training columns from the model (assuming it's stored as an attribute)
        training_columns = model.feature_names_  #if the feature names are stored in the model
        if training_columns is None:
            print(f"Error: Model does not contain feature names.")
            return

        # Preprocess the fresh data
        preprocessed_df = preprocess_fresh_data(fresh_df, training_columns)
        if preprocessed_df is None:
            return

        # Make predictions
        predictions = predict_completion(model, preprocessed_df)
        if predictions is None:
            return

        # Add predictions to the DataFrame
        fresh_df['predicted_completion_probability'] = predictions

        # Print the results
        print("Predictions:")
        print(fresh_df[['event_id', 'predicted_completion_probability']].to_string(index=False))

    except psycopg2.Error as e:
        print(f"Error connecting to or querying the database: {e}")
    except EnvironmentError as e:
        print(f"Configuration Error: {e}")
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")



if __name__ == "__main__":
    main()
