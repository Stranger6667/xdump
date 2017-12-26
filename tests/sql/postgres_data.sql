INSERT INTO
  groups (name)
VALUES
  ('Admin'),
  ('User');
INSERT INTO
  employees (first_name, last_name, manager_id, group_id)
VALUES
  ('John', 'Doe', NULL, 1),
  ('John', 'Black', 1, 1),  -- Has no subordinates
  ('John', 'Smith', 1, 1),
  ('John', 'Brown', 3, 2),
  ('John', 'Snow', 3, 2);
INSERT INTO
  tickets (author_id, subject, message)
VALUES
  (1, 'Sub 1', 'Message 1'),
  (2, 'Sub 2', 'Message 2'),
  (2, 'Sub 3', 'Message 3'),
  (2, 'Sub 4', 'Message 4'),
  (3, 'Sub 5', 'Message 5');
