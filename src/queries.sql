-- number of books written each year by an author
WITH author AS (
    SELECT
        id
    FROM authors
    WHERE id = '< insert id here >'
)
SELECT
    first_publish_year, 
    COUNT(*) AS total_books_published
FROM author a 
    JOIN books b ON a.id=b.author_id
GROUP BY first_publish_year
;

-- average number of books written by an author per year
WITH author AS (
    SELECT
        id
    FROM authors
    WHERE id = '< insert id here >'
)
SELECT
    a.id,
    ROUND(
        COUNT(*) / COUNT(DISTINCT first_publish_year)
    , 2) AS avg_books_published_per_year
FROM author a 
    JOIN books b ON a.id=b.author_id
GROUP BY a.id
;

-- books published by year and total books published by each author
SELECT
    a.full_name,
    book_name,
    first_publish_year,
    COUNT(*) OVER (PARTITION BY a.full_name, first_publish_year) AS books_published_this_year,
    COUNT(*) OVER (PARTITION BY a.full_name) AS total_books_published

FROM authors a 
    JOIN books b ON a.id=b.author_id 
;
