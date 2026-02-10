Feature: Seerr issue management via Matrix

  Background:
    Given a running Matrix homeserver
    And a running PostgreSQL database

  Scenario: A created Seerr issue posts a message in Matrix
    Given a room "#test-issue-created" exists
    And the bot is started and connected to room "#test-issue-created:localhost"
    When Seerr sends an "ISSUE_CREATED" webhook with:
      | issue_id    | 42                        |
      | subject     | Video playback problem    |
      | message     | The video won't load      |
      | reported_by | alice                     |
    Then a message appears in "#test-issue-created" containing "Video playback problem"
    And the message contains "alice"

  Scenario: Resolving an issue posts in the thread and adds a reaction
    Given a room "#test-issue-resolved" exists
    And the bot is started and connected to room "#test-issue-resolved:localhost"
    And Seerr sends an "ISSUE_CREATED" webhook with:
      | issue_id    | 43                     |
      | subject     | Missing subtitles      |
      | message     | No French subtitles    |
      | reported_by | bob                    |
    And a message appears in "#test-issue-resolved" containing "Missing subtitles"
    When Seerr sends an "ISSUE_RESOLVED" webhook with:
      | issue_id     | 43                           |
      | subject      | Missing subtitles            |
      | comment      | Subtitles added manually     |
      | commented_by | admin                        |
    Then a threaded reply appears on the original message containing "Subtitles added manually"
    And the original message has a "✅" reaction

  Scenario: A comment on an issue is posted in the thread
    Given a room "#test-issue-comment" exists
    And the bot is started and connected to room "#test-issue-comment:localhost"
    And Seerr sends an "ISSUE_CREATED" webhook with:
      | issue_id    | 44                        |
      | subject     | Movie not available       |
      | message     | The movie won't show up   |
      | reported_by | charlie                   |
    And a message appears in "#test-issue-comment" containing "Movie not available"
    When Seerr sends an "ISSUE_COMMENT" webhook with:
      | issue_id     | 44                        |
      | subject      | Movie not available       |
      | comment      | Looking into the problem  |
      | commented_by | admin                     |
    Then a threaded reply appears on the original message containing "Looking into the problem"
    And the threaded reply contains "admin"

  Scenario: Admin resolves issue via Matrix command
    Given a room "#test-admin-resolve" exists
    And the bot is started and connected to room "#test-admin-resolve:localhost"
    And Seerr sends an "ISSUE_CREATED" webhook with:
      | issue_id    | 50                     |
      | subject     | Broken subtitles       |
      | message     | Subs out of sync       |
      | reported_by | alice                  |
    And a message appears in "#test-admin-resolve" containing "Broken subtitles"
    When the admin sends '!issues resolve "Subtitles fixed"' as a thread reply
    Then Seerr received a comment "Subtitles fixed" for issue 50
    And Seerr received a resolve request for issue 50
    And a threaded reply appears on the original message containing "resolved"

  Scenario: Reopening an issue removes the reaction and posts in the thread
    Given a room "#test-issue-reopened" exists
    And the bot is started and connected to room "#test-issue-reopened:localhost"
    And Seerr sends an "ISSUE_CREATED" webhook with:
      | issue_id    | 45                     |
      | subject     | Bad audio quality      |
      | message     | Crackling sound        |
      | reported_by | dave                   |
    And a message appears in "#test-issue-reopened" containing "Bad audio quality"
    And Seerr sends an "ISSUE_RESOLVED" webhook with:
      | issue_id     | 45                     |
      | subject      | Bad audio quality      |
      | comment      | File replaced          |
      | commented_by | admin                  |
    And the original message has a "✅" reaction
    When Seerr sends an "ISSUE_REOPENED" webhook with:
      | issue_id    | 45                     |
      | subject     | Bad audio quality      |
      | reported_by | dave                   |
    Then a threaded reply appears on the original message containing "reopened"
    And the original message no longer has a "✅" reaction
