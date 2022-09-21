defmodule ExkafTest do
  use ExUnit.Case
  doctest Exkaf

  test "greets the world" do
    assert Exkaf.hello() == :world
  end
end
