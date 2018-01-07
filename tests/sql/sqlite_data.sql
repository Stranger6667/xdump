INSERT INTO
  groups (id, name)
VALUES
  (1, 'Admin'),
  (2, 'User');
INSERT INTO
  employees (id, first_name, last_name, manager_id, group_id)
VALUES
  (1, 'John', 'Doe', NULL, 1),
  (2, 'John', 'Black', 1, 1),  -- Has no subordinates
  (3, 'John', 'Smith', 1, 1),
  (4, 'John', 'Brown', 3, 2),
  (5, 'John', 'Snow', 3, 2);
INSERT INTO
  tickets (id, author_id, subject, message)
VALUES
  (1, 1, 'Sub 1', 'Message 1'),
  (2, 2, 'Sub 2', 'Message 2'),
  (3, 2, 'Sub 3', 'Message 3'),
  (4, 2, 'Sub 4', 'Message 4'),
  (5, 3, 'Sub 5', 'Message 5');
