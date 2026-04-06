from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from datetime import date
from typing import List, Optional  
import random

app = FastAPI(title="Property Management API")

# Enable CORS - Required for frontend (IA 10) to call this API 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "reflected-drake-489015-c3"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# --- Request Models for POST Methods ---

class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: str

class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: str
    description: str

class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float

# ---------------------------------------------------------------------------
# Properties Req
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property_by_id(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns a single property by ID[cite: 150]."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return dict(results[0])

# --- Income (Required) ---

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all income records for a property[cite: 152]."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/income/{property_id}", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Creates a new income record with a guaranteed unique ID."""
    
    # 1. Find the current maximum ID in the table
    max_id_query = f"SELECT MAX(income_id) as max_id FROM `{PROJECT_ID}.{DATASET}.income`"
    query_job = bq.query(max_id_query)
    results = list(query_job.result())
    
    # 2. Increment it by 1 (or start at 1 if table is empty)
    current_max = results[0]['max_id'] if results[0]['max_id'] is not None else 0
    new_id = current_max + 1

    # 3. Perform the Insert
    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (income_id, property_id, amount, date, description)
        VALUES ({new_id}, {property_id}, {income.amount}, '{income.date}', '{income.description}')
    """
    
    try:
        bq.query(insert_query).result()
        return {"message": "Income record created successfully", "income_id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Insert Failed: {str(e)}")

# --- Expenses (Required) ---

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all expense records for a property[cite: 155]."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/expenses/{property_id}", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Creates a new expense record with a guaranteed unique ID."""
    
    # 1. Find the current maximum ID in the expenses table
    max_id_query = f"SELECT MAX(expense_id) as max_id FROM `{PROJECT_ID}.{DATASET}.expenses`"
    query_job = bq.query(max_id_query)
    results = list(query_job.result())
    
    # 2. Increment by 1 (default to 1 if the table is empty)
    current_max = results[0]['max_id'] if results[0]['max_id'] is not None else 0
    new_id = current_max + 1

    # 3. Perform the Insert with all required fields from IA 8 and seed data
    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (expense_id, property_id, amount, date, category, vendor, description)
        VALUES ({new_id}, {property_id}, {expense.amount}, '{expense.date}', '{expense.category}', '{expense.vendor}', '{expense.description}')
    """
    
    try:
        bq.query(insert_query).result()
        return {"message": "Expense record created successfully", "expense_id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Insert Failed: {str(e)}")

# --- Custom Additional Endpoints ---

@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns total income, expenses, and net profit for a property."""
    query = f"""
        SELECT 
            (SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}) as total_income,
            (SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}) as total_expenses
    """
    row = dict(list(bq.query(query).result())[0])
    income = row['total_income'] or 0
    expenses = row['total_expenses'] or 0
    return {
        "property_id": property_id,
        "total_income": income,
        "total_expenses": expenses,
        "net_profit": income - expenses
    }

@app.get("/income")
def get_all_income(bq: bigquery.Client = Depends(get_bq_client)):
    """Master ledger: retrieves all income records portfolio-wide."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.get("/expenses")
def get_all_expenses(bq: bigquery.Client = Depends(get_bq_client)):
    """Master ledger: retrieves all expense records portfolio-wide."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.get("/portfolio/stats")
def get_portfolio_stats(bq: bigquery.Client = Depends(get_bq_client)):
    """Returns global metrics like total revenue and unit count."""
    query = f"""
        SELECT 
            SUM(monthly_rent) as potential_monthly_revenue, 
            COUNT(*) as unit_count, 
            AVG(monthly_rent) as avg_rent 
        FROM `{PROJECT_ID}.{DATASET}.properties`
    """
    results = list(bq.query(query).result())
    return dict(results[0])

@app.get("/analytics/yield/{property_id}")
def get_property_yield(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Calculates the Gross Rental Yield for a property.
    Formula: (Total Income Received / (Monthly Rent * 12)) * 100
    """
    # 1. Get the property's current monthly rent
    prop_query = f"SELECT monthly_rent FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    prop_results = list(bq.query(prop_query).result())
    
    if not prop_results:
        raise HTTPException(status_code=404, detail="Property not found")
    
    monthly_rent = prop_results[0]['monthly_rent']
    
    # 2. Get the total historical income for this property
    income_query = f"SELECT SUM(amount) as total_income FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}"
    income_results = list(bq.query(income_query).result())
    total_income = income_results[0]['total_income'] or 0

    # 3. Calculate Yield (assuming we want to see how current rent compares to historical intake)
    # Note: In a real scenario, this might involve property purchase price, 
    # but for Alex's MVP, we'll use annual expected vs actual.
    annual_expected = monthly_rent * 12
    yield_percentage = (total_income / annual_expected) * 100 if annual_expected > 0 else 0

    return {
        "property_id": property_id,
        "monthly_rent": monthly_rent,
        "total_historical_income": total_income,
        "gross_yield_index": round(yield_percentage, 2),
        "status": "Performing" if yield_percentage > 90 else "Underperforming"
    }

# --- Property Management (Closing the User Story Gaps) ---

@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """As a landlord, I want to add a new property (User Story 2)"""
    # 1. Generate New ID
    max_id_q = f"SELECT MAX(property_id) as max_id FROM `{PROJECT_ID}.{DATASET}.properties`"
    res = list(bq.query(max_id_q).result())
    new_id = (res[0]['max_id'] or 0) + 1

    # 2. Insert into BigQuery
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties` 
        (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
        VALUES ({new_id}, '{prop.name}', '{prop.address}', '{prop.city}', '{prop.state}', 
                '{prop.postal_code}', '{prop.property_type}', '{prop.tenant_name}', {prop.monthly_rent})
    """
    bq.query(query).result()
    return {"message": "Property added successfully", "property_id": new_id}

@app.put("/properties/{property_id}")
def update_property(property_id: int, prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """As a landlord, I want to edit property details (User Story 3)"""
    # First, check if it exists
    check_q = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    if not list(bq.query(check_q).result()):
        raise HTTPException(status_code=404, detail="Property not found")

    # Perform the update
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET name = '{prop.name}', 
            address = '{prop.address}', 
            city = '{prop.city}', 
            state = '{prop.state}', 
            postal_code = '{prop.postal_code}', 
            property_type = '{prop.property_type}', 
            tenant_name = '{prop.tenant_name}', 
            monthly_rent = {prop.monthly_rent}
        WHERE property_id = {property_id}
    """
    bq.query(query).result()
    return {"message": f"Property {property_id} updated successfully"}