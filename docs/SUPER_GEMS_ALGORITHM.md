# Super Gems Analysis Algorithm Documentation

## Overview

The Super Gems analysis system is designed to identify exceptional Hacker News posts that demonstrate high technical merit and community value. This document provides a detailed explanation of the algorithm, scoring methodology, and LLM prompts used to evaluate posts.

## Algorithm Architecture

### 1. Candidate Selection
The system first identifies potential "gems" - posts from low-karma authors (typically < 100 karma) that have already been flagged as hidden gems by the main system. These candidates undergo deeper analysis to determine if they qualify as "Super Gems."

### 2. Multi-Modal Analysis Pipeline

#### Phase 1: Content Preparation
- **Text Extraction**: Post content is extracted, with URL content fetched if post text is minimal
- **GitHub Detection**: Automatic detection of GitHub repository links for additional code analysis
- **Content Sanitization**: Text is sanitized to prevent prompt injection and formatting issues

#### Phase 2: LLM-Based Evaluation
The system uses Google's Gemini 2.5 Flash Lite model with specific configuration:
- **Temperature**: 0.1 (low creativity, high consistency)
- **Top-p**: 0.95
- **Top-k**: 40
- **Max Tokens**: 8192

#### Phase 3: Enhanced GitHub Repository Analysis
When GitHub repositories are detected, comprehensive analysis includes:
- **6 GitHub API calls** per repository for detailed metrics
- Repository metadata (stars, forks, issues, contributors, recent commits)
- File structure analysis (tests, CI/CD, documentation, project files)
- README quality assessment and license evaluation
- Factual implementation quality scoring based on measurable development practices

## Scoring Methodology

### Core Evaluation Dimensions

The algorithm evaluates posts across five primary dimensions, each scored from 0.0 to 1.0:

#### 1. Technical Innovation (Weight: 25%)
**Definition**: How novel or innovative is the technical approach?

**Evaluation Criteria**:
- Use of cutting-edge technologies or methodologies
- Novel solutions to existing problems
- Creative technical implementations
- Advancement of state-of-the-art in the domain

**Examples of High Scores**:
- New algorithms or data structures
- Novel applications of existing technologies
- Breakthrough implementations

#### 2. Problem Significance (Weight: 25%)
**Definition**: How important is the problem being solved?

**Evaluation Criteria**:
- Scale of impact on developer community
- Frequency of the problem encountered
- Current lack of good solutions
- Business or technical importance

**Examples of High Scores**:
- Developer productivity tools
- Solutions to widespread technical pain points
- Infrastructure improvements
- Security enhancements

#### 3. Implementation Quality (Weight: 20%) - **FACTUAL ASSESSMENT**
**Definition**: Objective assessment based on measurable GitHub repository metrics.

**Factual Evaluation Criteria**:
- **Repository Health (40%)**: GitHub stars, recent commits, issue management
- **Documentation Quality (30%)**: README length/quality, docs directory, license presence  
- **Project Structure (20%)**: Tests, CI/CD, requirements files, language diversity
- **Development Activity (10%)**: Contributors, forks, community engagement

**Note**: This dimension is calculated using factual GitHub API data, not LLM speculation.

#### 4. Community Value (Weight: 20%)
**Definition**: How valuable would this be to the Hacker News developer community?

**Evaluation Criteria**:
- Relevance to HN audience interests
- Educational value
- Practical applicability
- Discussion-worthy content
- Knowledge sharing potential

#### 5. Uniqueness (Weight: 10%)
**Definition**: How unique is this compared to existing solutions?

**Evaluation Criteria**:
- Differentiation from existing tools
- First-mover advantage
- Unique feature set
- Novel approach to common problems

### Bonus/Penalty System

#### Bonuses (Added to Base Score)
- **Open Source**: +0.10 (Encourages knowledge sharing)
- **Working Demo**: +0.05 (Demonstrates functionality)
- **GitHub Stars > 10**: +0.05 (Early community validation)
- **Outdated Knowledge Correction**: +0.15 (Compensates for LLM knowledge cutoff issues)

#### Penalties (Subtracted from Base Score)
- **Commercial Focus**: -0.15 (Reduces pure promotional content)
- **Poor Documentation**: -0.05 (Quality expectation)

### Final Score Calculation

```python
base_score = (
    technical_innovation * 0.25 +
    problem_significance * 0.25 +
    implementation_quality * 0.20 +
    community_value * 0.20 +
    uniqueness * 0.10
)

final_score = base_score + bonuses - penalties
final_score = min(max(final_score, 0), 1)  # Clamp to [0,1]
```

### Knowledge Cutoff Compensation

The system includes logic to detect when the LLM may have penalized a post due to outdated knowledge about recent releases or technologies. If specific phrases are detected in the reasoning (like "doesn't exist", "not released", "misleading"), the system applies a corrective boost of +0.15.

## LLM Prompts

### Main Analysis Prompt

The primary prompt guides the LLM through comprehensive evaluation:

```
Analyze this Hacker News post for its value to the developer community.

IMPORTANT CONTEXT:
- Today's date: {current_date}
- My knowledge may have a cutoff date - avoid penalizing posts for claims about recent releases
- Focus on technical merit, implementation quality, and developer value rather than factual verification
- If a post claims something "impossible" exists, evaluate it based on the described functionality

Post Title: {title}
Post URL: {url}
Post Text: {text}
Author Karma: {author_karma}

Please evaluate the following aspects (score 0-1):

1. Technical Innovation: How novel or innovative is the technical approach?
2. Problem Significance: How important is the problem being solved?
3. Implementation Quality: Based on available information, how well-executed is this?
4. Community Value: How valuable would this be to the HN developer community?
5. Uniqueness: How unique is this compared to existing solutions?

Also determine:
- Is this open source? (look for GitHub links, license mentions)
- Does it have a working demo?
- Is the documentation good?
- Is this primarily a commercial/paid product? (negative signal)
- What similar tools/solutions exist?
- What are the key strengths?
- What are potential concerns?

If this is a Show HN post about a developer tool, give extra weight to:
- Solves real developer pain points
- Good technical documentation
- Open source with permissive license
- Active development
- Not just another wrapper around existing APIs

EVALUATION GUIDELINES:
- Do NOT heavily penalize posts for mentioning new releases or models you're unfamiliar with
- Focus on the VALUE PROPOSITION and TECHNICAL APPROACH rather than existence verification
- If uncertain about recent releases, give benefit of the doubt and focus on implementation merit
- Avoid using phrases like "misleading" or "doesn't exist" for recent technology claims
```

**Key Design Principles**:
1. **Factual Implementation Assessment**: Implementation quality calculated from measurable GitHub metrics only
2. **Recency Awareness**: Explicitly instructs the LLM to avoid penalizing recent developments  
3. **Community Focus**: Tailored specifically for HN developer audience
4. **Open Source Preference**: Built-in bias toward open source solutions
5. **Anti-Commercial Bias**: Reduces weight of purely promotional content
6. **No Algorithmic Speculation**: Podcasts avoid numerical scores, focus on factual data and qualitative analysis

### GitHub Analysis Prompt

For posts with associated GitHub repositories:

```
Analyze this GitHub repository for code quality and project health:

Repository: {repo_url}
README Content: {readme}
File Structure: {file_structure}
Recent Commits: {recent_commits}

Evaluate:
1. Code Quality (0-1): Based on structure, organization, patterns
2. README Quality (0-1): Clarity, completeness, examples

Return as JSON: {"code_quality": 0.0-1.0, "readme_quality": 0.0-1.0}
```

## Quality Assurance Measures

### 1. Consistency Controls
- Low temperature (0.1) for consistent scoring
- Structured JSON output format
- Multiple parsing attempts with fallbacks

### 2. Bias Mitigation
- Explicit instructions to avoid recency bias
- Compensation mechanisms for knowledge cutoff issues
- Focus on technical merit over popularity

### 3. Error Handling
- Graceful degradation when content is unavailable
- Default values for missing data
- Comprehensive exception handling

## Validation and Calibration

### Score Distribution
The system is calibrated to produce meaningful score distributions:
- **0.0-0.3**: Below average posts
- **0.3-0.5**: Average posts with some merit
- **0.5-0.7**: Good posts worth attention
- **0.7-0.85**: Excellent posts (Super Gem candidates)
- **0.85-1.0**: Exceptional posts (Top-tier Super Gems)

### Human Validation
The system includes provisions for human review:
- Detailed reasoning provided for each score
- Key strengths and concerns explicitly listed
- Similar tools identified for context
- Full audit trail of scoring factors

## Performance Characteristics

- **Processing Speed**: ~10-15 seconds per post analysis
- **API Efficiency**: Optimized for Gemini Flash model (fast, cost-effective)
- **Accuracy**: Calibrated against manual curation samples
- **Consistency**: Low-temperature settings ensure reproducible results

## Future Improvements

1. **Multi-Model Validation**: Cross-validation with different LLM models
2. **Community Feedback Loop**: Integration of community voting/feedback
3. **Dynamic Weighting**: Adaptive weights based on post categories
4. **Temporal Analysis**: Consideration of posting time and discussion velocity

## Conclusion

The Super Gems algorithm represents a sophisticated approach to automated content curation, balancing technical rigor with community value assessment. By combining structured LLM evaluation with rule-based scoring adjustments, it provides a scalable method for identifying exceptional technical content that might otherwise be overlooked due to author karma limitations.

The system's emphasis on transparency, bias mitigation, and continuous improvement makes it a robust tool for elevating high-quality technical discussions in the Hacker News community.