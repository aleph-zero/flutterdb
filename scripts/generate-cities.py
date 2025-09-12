import json
import random

# Predefined real city names and descriptions
cities_descriptions = [
    ("New York", "Largest US city, known for skyline and Central Park"),
    ("London", "Historic capital with museums and the Thames"),
    ("Paris", "City of light, fashion, and the Eiffel Tower"),
    ("Tokyo", "Bustling capital with tech and tradition"),
    ("Berlin", "German capital known for history and nightlife"),
    ("Toronto", "Diverse Canadian city by Lake Ontario"),
    ("Melbourne", "Australian city known for coffee culture"),
    ("Bangkok", "Thai capital with temples and street food"),
    ("Buenos Aires", "Argentinian city of tango and steak"),
    ("Seoul", "South Korea's high-tech and cultural hub"),
    ("Amsterdam", "Dutch city with canals and bicycles"),
    ("Rome", "Italian capital with ancient ruins"),
    ("Istanbul", "Straddles Europe and Asia with rich history"),
    ("Barcelona", "Spanish city famous for Gaudí and beaches"),
    ("Dubai", "UAE city with skyscrapers and desert charm"),
    ("Chicago", "Known for deep-dish pizza and Lake Michigan"),
    ("Lima", "Capital of Peru with colonial architecture"),
    ("Oslo", "Norwegian capital known for fjords and design"),
    ("Helsinki", "Finnish capital with modern design"),
    ("Cape Town", "Coastal city beneath Table Mountain"),
]

# Create a function to generate random city names
def generate_fake_city():
    prefixes = ["San", "Fort", "New", "Lake", "Port", "North", "South", "West", "East"]
    roots = ["ville", "burg", "caster", "ton", "polis", "ford", "ham", "mouth", "field"]
    name = random.choice(prefixes) + random.choice([" ", ""]) + random.choice(roots).capitalize()
    return name

# Choose random number of entries
num_entries = random.randint(3001, 4567)

# Generate NDJSON entries
ndjson_lines = []
for _ in range(num_entries):
    if random.random() < 0.7:
        city, description = random.choice(cities_descriptions)
    else:
        city = generate_fake_city()
        description = "A fictional city with a vibrant community and scenic views"
    description = description[:100]  # Enforce 100 character limit
    population = random.randint(100_000, 5_000_000)
    entry = {
        "city": city,
        "description": description,
        "population": population
    }
    ndjson_lines.append(json.dumps(entry))

# Save to NDJSON file
output_path = "./cities.ndjson"
with open(output_path, "w") as f:
    for line in ndjson_lines:
        f.write(line + "\n")

output_path

