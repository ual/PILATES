.headers off 
.mode csv 

.output "artificial_count_temp.csv" 
select * from artificial_count;

.output "gap_calculations_temp.csv" 
select * from gap_calculations;

.output "gap_breakdown_temp.csv" 
select * from gap_breakdown;

.exit