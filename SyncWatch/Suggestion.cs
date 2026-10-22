using System;

namespace SyncWatch;

public record SuggestionDto(string? Url, string? Title);
public record Suggestion(int Id, string Url, string Title, DateTime CreatedUtc);
