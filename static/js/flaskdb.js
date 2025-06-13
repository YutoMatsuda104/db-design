/*
 * A Sample Web-DB Application for DB-DESIGN lecture
 * Copyright (C) 2022 Yasuhiro Hayashi
 */
$(function() {
  $("form").submit(function() {
    $(this).find(":submit").prop("disabled", true);
    setTimeout(function() {
      $(this).find(":submit").prop("disabled", false);
    }, 10000);
  });

  $("#cancel").on("click", function() {
    history.back();
  })
});
