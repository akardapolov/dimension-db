package ru.dimension.db.exception;

public class TableNameEmptyException extends Exception {
  public TableNameEmptyException(String message) {
    super(message);
  }
}
