import yaml
import snowflake.connector
from jinja2 import Environment, FileSystemLoader
import os

# Function to load YAML configuration
def load_config(config_path):
    """Load YAML configuration file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Function to render SQL using Jinja
def render_template(template_file, context):
    """Render Jinja template with values from YAML."""
    env = Environment(loader=FileSystemLoader('.'), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template(template_file)
    return template.render(context)

# Function to execute SQL script in Snowflake
def execute_in_snowflake(sql_script, config):
    """Execute SQL script in Snowflake."""
    print("\n🔄 Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user="vamshika12",  # Replace with your Snowflake username
        password="Asvmsh@12Agsgr@27",  # Replace with your Snowflake password
        account="dgeyskn-tob19336",  # Example: "abc123.us-east-1"
        role=config["role"],  # Use the role from YAML
        warehouse=config["warehouse"],  # Use the warehouse from YAML
        database=config["database"],  # Use the database from YAML
        schema=config["schema"]  # Use the schema from YAML
    )

    cur = conn.cursor()
    print("✅ Connected to Snowflake!")

    print("\n📤 Executing SQL script...\n")
    
    # Execute each SQL statement separately
    for statement in sql_script.split(';'):
        if statement.strip():
            cur.execute(statement)

    print("\n✅ SQL script executed successfully in Snowflake.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    # 1️⃣ Choose environment
    environment = input("Enter environment (DEV/PROD): ").strip().upper()
    
    # 2️⃣ Load the respective config file
    config_file = f"{environment.lower()}_config.yaml"
    if not os.path.exists(config_file):
        print(f"❌ Error: Configuration file '{config_file}' not found.")
        exit(1)
    
    config = load_config(config_file)
    
    # 3️⃣ Render Jinja template to generate SQL
    sql_script = render_template("manage_env.j2", config)

    # 4️⃣ Save the generated SQL script
    sql_file = f"generated_{environment.lower()}.sql"
    with open(sql_file, "w") as file:
        file.write(sql_script)
    
    print(f"\n✅ SQL script saved as: {sql_file}")

    # 5️⃣ Execute the generated SQL script in Snowflake
    execute_in_snowflake(sql_script, config)
