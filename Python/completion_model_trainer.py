"""
Completion Model Trainer Script

This script connects to a PostgreSQL database, retrieves data for training a machine learning model, 
and trains a Logistic Regression model to predict whether a user event is a "completion" event. 
The script evaluates the model's performance using metrics such as AUC-ROC and Precision-Recall curves 
and visualizes the results.

Key Features:
1. Connects to a PostgreSQL database to fetch training data.
2. Preprocesses the data by handling missing values and encoding categorical variables.
3. Trains a Logistic Regression model with balanced class weights.
4. Evaluates the model using classification metrics and visualizations.
5. Prints feature importances for interpretability.

Dependencies:
- pandas: For data manipulation.
- scikit-learn: For machine learning and evaluation.
- matplotlib: For plotting evaluation metrics.
- psycopg2: For connecting to the PostgreSQL database.

Usage:
1. Ensure all dependencies are installed (`pip install pandas scikit-learn matplotlib psycopg2`).
2. Set the `POSTGRES_PASSWORD` environment variable with the database password.
3. Run the script to train and evaluate the model.

"""

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score, roc_curve, precision_recall_curve, average_precision_score
import matplotlib.pyplot as plt
import psycopg2
import os
from io import StringIO
import subprocess
from sklearn.impute import SimpleImputer
import warnings

def install_dependencies():
    """
    Installs required Python packages if they are not already installed.
    """
    try:
        pd.test = None  # Check if pandas is installed
    except ImportError:
        print("pandas is not installed. Installing...")
        subprocess.check_call(['pip', 'install', 'pandas'])
    try:
        from sklearn.model_selection import train_test_split  # Check if scikit-learn is installed
    except ImportError:
        print("scikit-learn is not installed. Installing...")
        subprocess.check_call(['pip', 'install', 'scikit-learn'])
    try:
        import matplotlib.pyplot as plt  # Check if matplotlib is installed
    except ImportError:
        print("matplotlib is not installed. Installing...")
        subprocess.check_call(['pip', 'install', 'matplotlib'])
    try:
        import psycopg2  # Check if psycopg2 is installed
    except ImportError:
        print("psycopg2 is not installed. Installing...")
        subprocess.check_call(['pip', 'install', 'psycopg2'])

def load_data(conn, query):
    """
    Loads data from the database into a Pandas DataFrame.

    Args:
        conn: psycopg2 connection object.
        query: SQL query to execute.

    Returns:
        pandas.DataFrame: DataFrame containing the query results, or None on error.
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        print(f"Error loading data: {e}")
        return None

def train_and_evaluate_model(df):
    """
    Trains a Logistic Regression model, evaluates its performance, and prints a report.

    Args:
        df: pandas.DataFrame containing the training data.
    """
    # Prepare the data
    X = df.drop(['event_id', 'event_ts', 'is_completion', 'user_genre_preference'], axis=1)
    y = df['is_completion']

    # Convert categorical variables to dummy variables
    X = pd.get_dummies(X, dummy_na=False)

    # Impute missing values using the mean
    imputer = SimpleImputer(strategy='mean')
    X = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # Train the model
    model = LogisticRegression(random_state=42, solver='liblinear', class_weight='balanced')
    model.fit(X_train, y_train)

    # Evaluate the model
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Print the classification report
    print("Classification Report:")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        print(classification_report(y_test, y_pred, zero_division=0))

    # Print the AUC-ROC score
    auc_roc = roc_auc_score(y_test, y_pred_proba)
    print(f"AUC-ROC Score: {auc_roc:.3f}")

    # Plot the ROC curve
    fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, label=f'AUC = {auc_roc:.3f}')
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curve')
    plt.legend()
    plt.show()

    # Plot the Precision-Recall curve
    precision, recall, _ = precision_recall_curve(y_test, y_pred_proba)
    average_precision = average_precision_score(y_test, y_pred_proba)
    plt.figure(figsize=(8, 6))
    plt.plot(recall, precision, label=f'AP = {average_precision:.3f}')
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('Precision-Recall Curve')
    plt.legend()
    plt.show()
    
    # Print the feature importances
    print("\nFeature Importances:")
    for feature, importance in zip(X.columns, model.coef_[0]):
        print(f"{feature:30}: {importance:.3f}")

def main():
    """
    Main function to connect to the database, load data, and train the model.
    """
    install_dependencies()
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

        query = """SELECT * FROM completion_prediction_features"""
        df = load_data(conn, query)
        if df is not None:
            print(f"Loaded {len(df)} records from completion_prediction_features")
            train_and_evaluate_model(df)

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