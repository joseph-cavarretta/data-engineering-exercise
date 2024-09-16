-- number of books written each year by an author
WITH author AS (
    SELECT
        id
    FROM authors
    WHERE id = '< insert id here >'
)
SELECT
    first_publish_year, COUNT(*)
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
    ROUND(
        COUNT(*) / COUNT(DISTINCT first_publish_year)
    , 2) AS avg_books_published_per_year
FROM author a 
    JOIN books b ON a.id=b.author_id
;