declare @Min int, @Max int;

select @Min = 1, @Max = 43;

select round(((@Max - @Min - 1) * rand() + @Min), 0)