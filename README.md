# Chicago Requests
Bulk downloader for 311 Requests from the Chicago Open311 API.

## To install:
```console
> pip install chicagorequests
```

## To use

### Today's Requests
To download today's requests, run:
```console
> chicagorequests
```

The program will output the requests as line-delimited json. To turn this into a
standard json file, you can use a program like `jq`.

```console
> chicagorequests | jq -s '.'
```

### Date Range Request
To download all requests within a time span: 

```console
> chicagorequests --start-date=2021-01-01 --end-date=2021-01-14
```

### Limit to specific request types
To download grafitti removal requests made today:

```console
> chicagorequests -t graffiti_removal 
```

You can specify more than one type of request

```console
> chicagorequests -t graffiti_removal -t pothole 
```

### To get help
```console
> chicagorequests --help
Usage: chicagorequests [OPTIONS]

  Download service requests from the Chicago Open311 API. By default, today's
  requests of all types. Will write service requests as line-delimited JSON to
  stdout.

Options:
  -s, --start-date [%Y-%m-%d]  the first day of the time range to check
  -e, --end-date [%Y-%m-%d]    the last day of the time range to check
  -t, --request-type TEXT      service types to fetch
  -v, --verbose                verbosity level
  --list-request-types         list valid request types
  --help                       Show this message and exit.
```

### To see all request types:

```console
> chicagorequests --list-request-types
type                             definition
-------------------------------  ----------------------------------------
abandoned_vehicle                Abandoned Vehicle Complaint
affordable_rental_housing_list   Affordable Rental Housing List
air_conditioning_violation       Air Cond./Refrigeration Violation
air_pollution                    Air Pollution, Odor or Dust Complaint
alley_grading                    Alley Grading-Unimproved
alley_light_out                  Alley Light Out Complaint
alley_pothole                    Alley Pothole Complaint
alley_sewer                      Alley Sewer Inspection Request
animal_abandoned                 Animal Abandoned
animal_business                  Animal Business Complaint
animal_fighiting                 Animal Fighting Suspicion
animal_trap                      Animal In Trap Complaint
asbestos                         Asbestos Complaint
bees_wasps                       Bee/Wasp Removal
bicycle_request                  Bicycle Request/Complaint
bike_lane_post                   Bike Lane Post Repair
black_garbage_cart_removal       Black Garbage Cart Removal
blue_recycling_cart              Blue Recycling Cart
board_of_education               Board of Education
bridges                          Bridges and Viaducts (All Types)
public_facility                  Building Public Facility Violation
boiler                           Buildings - Boiler Violation
electrical                       Buildings - Electrical Violation
elevator                         Buildings - Elevator Violation
furnace                          Buildings - Furnace Violation
plumbing                         Buildings - Plumbing Violation
bulk_pickup                      Bulk Pickup
vintage_home                     Bungalow/Vintage Home Information
                                 Request
business_complaint               Business Complaints
cdot_constrcution                CDOT Construction Complaints
cdot_electrical                  CDOT Electrical Operations Construction
                                 Complaints
cha                              CHA Miscellaneous
taxi                             Cab Feedback
cable_cut                        Cable Cut
cable_tv                         Cable TV Complaint
check_for_leak                   Check for Leak
city_clerk_feedback              City Clerk Feedback Request
city_electrical_vault            City Electrical Vault
city_vehicle_sticker_violation   City Vehicle Sticker Violation
vacant_lot                       Clean Vacant Lot Request
green_program                    Clean and Green Program Request
fire_safety                      Commercial Fire Safety Inspection
                                 Request
city_vehicle_condition           Condition of City Vehicle
construction_complaint           Construction & Demolition Complaint
consumer_fraud                   Consumer Fraud Complaint
consumer_business                Consumer Retail Business Complaint
coyote                           Coyote Interaction Complaint
crisis_referral                  Crisis Referral
daycare_center                   Daycare Center
dead_animal_pickup               Dead Animal Pick-Up Request
dead_bird                        Dead Bird
disabled_parking                 Disabled Parking
divvy_bike_parking               Divvy Bike Parking Complaint
escooter_parking                 E-Scooter Parking Complaint
electrical_sign_inspection       Electrical Sign Inspection
equipment_noise                  Equipment Noise Complaint
fire_assistance                  Fire Assistance
fire_miscellaneous               Fire Miscellaneous
flooded_viaduct                  Flooded Viaduct
fly_dump_tires                   Fly Dump (Tires)
fly_dumping                      Fly Dumping Complaint
furniture_repair_cta             Furniture Repair - CTA
garbage_cart_maintenance         Garbage Cart Maintenance
gas_station                      Gas Station and Storage Tank Complaint
graffiti_removal                 Graffiti Removal Request
groceries                        Groceries
guardrail_maintenance            Guardrail and Roadside Protection
                                 Maintenance
gym_shoe_electrical_wire         Gym Shoe/Object On Electrical Wire
health_club                      Health Club
home_buyer_program_info          Home Buyer Program Info Request
homeless_prevention              Homeless Prevention
hotel_motel_health_dept          Hotel/Motel - Health Department
housing_inquiries                Housing Inquiries
how_is_my_driving                How's my driving?
hydrant_cap_missing              Hydrant Cap Missing
hydrant_check                    Hydrant Check
ice_snow_removal                 Ice and Snow Removal Request
illegal_dumping                  Illegal Dumping
in_ground_pedestrian_crossing    In-Ground Pedestrian Crossing Sign
                                 Repair
inaccurate_fuel_pump             Inaccurate Fuel Pump Complaint
inaccurate_retail_scales         Inaccurate Retail Scales Complaint
inspect_public_way               Inspect Public Way Request
landscape_median_maintenance     Landscape Median Maintenance
lead_inspection                  Lead Inspection Request
licensed_pharmaceutical_rep      Licensed Pharmaceutical Representative
                                 Complaint
liquor_establishment             Liquor Establishment Complaint
locate_service_bbox              Locate/ID Main/Service/BBox/Meter Vault
low_water_pressure               Low Water Pressure Complaint
midway_customer_service          Midway Customer Service
midway_facilities                Midway Facilities Complaint
missed_garbage_pickup            Missed Garbage Pick-Up Complaint
missing_lid_grate_cover          Missing Lid/Grate/Cover
mobile_food_vehicle              Mobile Food Vehicle
mosquito_spraying_inquiries      Mosquito Spraying Inquiries (Formerly
                                 West Nile Virus)
construction_rodent_inspection   New Excavation or Construction Rodent
                                 Siting Inspection
no_air_conditioning              No Air Conditioning
no_building_permit               No Building Permit and Construction
                                 Violation
no_solicitation                  No Solicitation Complaint
no_vehicle_idling                No Vehicle Idling (Diesel)
no_water                         No Water Complaint
nuisance_animal                  Nuisance Animal Complaint
ohare_customer_service           O'Hare Customer Service
ohare_facilities                 O'Hare Facilities Complaint
open_fire_hydrant                Open Fire Hydrant Complaint
outdated_merchandise             Outdated Merchandise Complaint
paid_sick_leave                  Paid Sick Leave Violation
park_repair_maintenance          Park Repair/Maintenance
park_rodent_abatement            Park Rodent Abatement
pavement_buckle_repair           Pavement Buckle Repair
pet_wellness_check               Pet Wellness Check Request
police_misc                      Police Miscellaneous
porch_inspection                 Porch Inspection Request
pothole                          Pothole in Street Complaint
bike_lane_debris_removal         Protected Bike Lane - Debris Removal
public_vehicle                   Public Vehicle/Valet Complaint
pushcart_food_vendor             Pushcart Food Vendor Complaint
recycling_inspection             Recycling Inspection Request
recycling_pickup                 Recycling Pick Up
red_light_camera                 Red Light Camera
relocated_vehicle                Relocated Vehicle
relocation                       Relocation Request
renters_foreclosure              Renters and Foreclosure Complaint
injured_animal_report            Report an Injured Animal
water_shut_off_occupied          Request Water Shut-Off Occupied Building
water_shut_off_vacant            Request Water Shut-off Vacant Building
fire_hydrant_custodian           Request to Install Custodian on Fire
                                 Hydrant
restaurant                       Restaurant Complaint
ridesharing                      Ridesharing Complaint
rats                             Rodent Baiting/Rat Complaint
sanitation_code_violation        Sanitation Code Violation
sanitation_tire_pickup           Sanitation Tire Pickup
sewer_cave_in_inspection         Sewer Cave-In Inspection Request
sewer_cleaning_inspection        Sewer Cleaning Inspection Request
sewer_outfall_investigation      Sewer Outfall Investigation
shared_cost_sidewalk             Shared Cost Sidewalk Program Request
vacation_rental                  Shared Housing/Vacation Rental Complaint
shelter                          Shelter Request
sidewalk_cafe                    Sidewalk Cafe Complaint
sidewalk_inspection              Sidewalk Inspection Request
sign_loading_standing_zone       Sign - Loading or Standing Zone (New
                                 Installation, Amendment, Removal)
sign_repair_base_bolt_removal    Sign Repair -  Base / Bolt Removal
sign_repair_all_other_signs      Sign Repair Request - All Other Signs
sign_repair_do_not_enter         Sign Repair Request - Do Not Enter Sign
sign_repair_one_way              Sign Repair Request - One Way Sign
sign_repair_residential_parking  Sign Repair Request - Residential Permit
                                 Parking
sign_repair_stop                 Sign Repair Request - Stop Sign
ramps_and_repairs_for_seniors    Small Accessible Repairs and Ramps for
                                 Seniors
smokeless_tobacco                Smokeless Tobacco at Sports Event
                                 Complaint
smoking_food_establishment       Smoking: Food Establishment
smoking_non_food_establishment   Smoking: Non-Food Establishment
snow_object_dibs_removal         Snow - Object/Dibs Removal Request
snow_protected_bike_lane         Snow Removal - Protected Bike Lane or
                                 Bridge Sidewalk
snow_uncleared_sidewalk          Snow – Uncleared Sidewalk Complaint
speed_camera                     Speed Camera
speed_hump_repair                Speed Hump Repair
spills_or_dumping                Spills or Dumping in Natural Water Ways
stray_animal                     Stray Animal Complaint
street_cleaning                  Street Cleaning Request
street_light_on_during_day       Street Light On During Day Complaint
street_light_out                 Street Light Out Complaint
street_light_pole_damage         Street Light Pole Damage Complaint
street_light_pole_door_missing   Street Light Pole Door Missing Complaint
street_paint_marking             Street Paint Marking Maintenance
swimming_pool_insp               Swimming Pool Insp
tanning_and_body_art             Tanning and Body Art Complaint
tobacco_general                  Tobacco - General Complaint
tobacco_sale_to_minors           Tobacco - Sale to Minors Complaint
toxic_hazardous_materials        Toxic and Hazardous Materials
traffic_calming_devices          Traffic Calming Devices – Existing
traffic_signal_out               Traffic Signal Out Complaint
traffic_signal_timing            Traffic Signal Timing
trap_pickup                      Trap Pick-Up
tree_debris_cleanup              Tree Debris Clean-Up Request
tree_emergency                   Tree Emergency
tree_planting                    Tree Planting Request
tree_removal                     Tree Removal Request
tree_trim                        Tree Trim Request
unwanted_animal                  Unwanted Animal
vacant_abandoned_building        Vacant/Abandoned Building Complaint
ventilation_violation            Ventilation Violation
viaduct_light_out                Viaduct Light Out Complaint
vicious_animal                   Vicious Animal Complaint
volunteer_network                Volunteer Network
wage_complaint                   Wage Complaint
water_on_street                  Water On Street Complaint
water_in_basement                Water in Basement Complaint
weed_removal                     Weed Removal Request
wildlife_in_home                 Wildlife Assistance in Living Quarters
wire_basket                      Wire Basket Request
wire_down                        Wire Down
yard_waste                       Yard Waste Pick-Up Request
```
