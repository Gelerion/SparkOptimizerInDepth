package com.gelerion.spark.examples.data

//https://relational.fit.cvut.cz/dataset/IMDb
object domain_data {

  //movies database
  def movies: Seq[Movie] = {
    Movie(1, "Black Panther", 2018, 1) ::
    Movie(2, "Avengers: Infinity War", 2018, 2) ::
    Movie(3, "Mary Poppins Returns", 2017, 3) ::
    Movie(4, "Green Book", 2017, 4) ::
    Movie(5, "The Favourite", 2016, 5) ::
    Nil
  }

  //roles database
  def roles: Seq[Role] = {
    Role(1, movieId = 1, "Actor")::
    Role(2, movieId = 1, "Actor")::
    Role(3, movieId = 1, "Assistant")::
    Role(1, movieId = 2, "Actor")::
    Role(4, movieId = 2, "Actor")::
    Nil
  }

  //actors database
  def actors: Seq[Actor] = {
    Actor(1, "Denis", "Richardson", 'M') ::
    Actor(2, "Karpet", "Jakovski", 'M') ::
    Actor(3, "Gilbert", "Morrison", 'M') ::
    Actor(4, "Marry", "Jane", 'M') ::
    Nil
  }
}

case class Movie(id: Int, name: String, year: Int, rank: Float)
case class Role(actorId: Int, movieId: Int, role: String)
case class Actor(id: Int, firstName: String, lastName: String, gender: Char)
